import json
import subprocess
from pathlib import Path
from pyspark.sql.functions import col
from functions.utility import create_table_if_not_exists, get_function, apply_job_type


def rescue_silver_table(mode, table_name, spark):
    """Rescue a silver table using either timestamps or version numbers.

    Parameters
    ----------
    mode : {"timestamp", "versionAsOf"}
        Determines how history is replayed.
    spark : pyspark.sql.SparkSession
    table_name : str
    """

    if mode not in {"timestamp", "versionAsOf"}:
        raise ValueError("mode must be 'timestamp' or 'versionAsOf'")

    settings_path = f"../layer_02_silver/{table_name}.json"
    settings = json.loads(Path(settings_path).read_text())
    settings = apply_job_type(settings)

    transform_function = get_function(settings["transform_function"])
    upsert_function = get_function(settings["upsert_function"])
    upsert = upsert_function(settings, spark)

    # Remove old destination table and checkpoint directory
    dst_path = settings.get("dst_table_path")
    if dst_path:
        subprocess.run(["rm", "-rf", dst_path], check=True)
    else:
        spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    checkpoint_location = settings["writeStreamOptions"]["checkpointLocation"]
    if checkpoint_location.startswith("/Volumes/") and checkpoint_location.endswith("_checkpoints/"):
        subprocess.run(["rm", "-rf", checkpoint_location], check=True)
    else:
        raise ValueError(f"Skipping checkpoint deletion, unsupported path: {checkpoint_location}")

    if mode == "timestamp":
        df = spark.read.format("delta").load(settings["src_table_path"]).orderBy("derived_ingest_time")
        times = [r[0] for r in df.select("derived_ingest_time").distinct().orderBy("derived_ingest_time").collect()]
        for i, timestamp in enumerate(times):
            batch = df.filter(col("derived_ingest_time") == timestamp)
            batch = transform_function(batch, settings, spark)
            if i == 0:
                if dst_path:
                    (
                        batch.limit(0)
                        .write.format("delta")
                        .option("delta.columnMapping.mode", "name")
                        .mode("overwrite")
                        .save(dst_path)
                    )
                else:
                    create_table_if_not_exists(batch, settings["dst_table_name"], spark)
            upsert(batch, i)
            print(f"{table_name}: Upserted batch {i} for time {timestamp}")
        print(f"{table_name}: Rescue completed in {len(times)} batches")

    else:  # mode == "versionAsOf"
        history = spark.sql(f"DESCRIBE HISTORY delta.`{settings['src_table_path']}`")
        max_version = history.agg({"version": "max"}).first()[0]
        print(f"{table_name}: Max version {max_version}")

        for version in range(0, max_version + 1):
            if version == 0:
                df = spark.read.format("delta").option("versionAsOf", version).load(settings["src_table_path"])
                df = transform_function(df, settings, spark)
                if dst_path:
                    (
                        df.limit(0)
                        .write.format("delta")
                        .option("delta.columnMapping.mode", "name")
                        .mode("overwrite")
                        .save(dst_path)
                    )
                else:
                    create_table_if_not_exists(df, settings["dst_table_name"], spark)
                print(f"{table_name}: Current version {version}")
                continue

            prev = spark.read.format("delta").option("versionAsOf", version - 1).load(settings["src_table_path"])
            cur = spark.read.format("delta").option("versionAsOf", version).load(settings["src_table_path"])
            df = cur.subtract(prev)
            df = transform_function(df, settings, spark)
            upsert(df, version - 1)
            print(f"{table_name}: Current version {version}")

        settings["readStreamOptions"]["startingVersion"] = max_version
        Path(settings_path).write_text(json.dumps(settings, indent=4))
        print(f"{table_name}: Updated settings with startingVersion {max_version}")


def rescue_silver_table_timestamp(table_name, spark):
    """Backwards compatible wrapper for ``rescue_silver_table`` using timestamps."""
    rescue_silver_table("timestamp", table_name, spark)


def rescue_silver_table_versionAsOf(table_name, spark):
    """Backwards compatible wrapper for ``rescue_silver_table`` using Delta versions."""
    rescue_silver_table("versionAsOf", table_name, spark)




def rescue_gold_table(table_name, spark):
    """Rebuild a gold table by replaying all versions of the source."""
    settings = json.loads(Path(f"../layer_03_gold/{table_name}.json").read_text())
    settings = apply_job_type(settings)
    settings["ingest_time_column"] = "derived_ingest_time"

    history = spark.sql(f"DESCRIBE HISTORY delta.`{settings['src_table_path']}`")
    max_version = history.agg({"version": "max"}).first()[0]
    print(f"{table_name}: Max version {max_version}")

    dst_path = settings.get("dst_table_path")
    if dst_path:
        subprocess.run(["rm", "-rf", dst_path], check=True)
    else:
        spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    
    transform_function = get_function(settings["transform_function"])
    write_function = get_function(settings["write_function"])

    for version in range(0, max_version + 1):
        df = spark.read.format("delta").option("versionAsOf", version).load(settings["src_table_path"])
        df = transform_function(df, settings, spark)

        if version == 0:
            if dst_path:
                (
                    df.limit(0)
                    .write.format("delta")
                    .option("delta.columnMapping.mode", "name")
                    .mode("overwrite")
                    .save(dst_path)
                )
            else:
                create_table_if_not_exists(df, settings["dst_table_name"], spark)
        else:
            write_function(df, settings, spark)

        print(f"Current version: {version}")




