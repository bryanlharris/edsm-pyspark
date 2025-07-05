import json
import subprocess
from pathlib import Path
from pyspark.sql.functions import col
from functions.utility import create_table_if_not_exists, get_function, apply_job_type


def rescue_silver_table(mode, spark, table_name):
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
    spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    checkpoint_location = settings["writeStreamOptions"]["checkpointLocation"]
    if checkpoint_location.startswith("/Volumes/") and checkpoint_location.endswith("_checkpoints/"):
        subprocess.run(["rm", "-rf", checkpoint_location], check=True)
    else:
        raise ValueError(f"Skipping checkpoint deletion, unsupported path: {checkpoint_location}")

    if mode == "timestamp":
        df = spark.read.table(settings["src_table_name"]).orderBy("derived_ingest_time")
        times = [r[0] for r in df.select("derived_ingest_time").distinct().orderBy("derived_ingest_time").collect()]
        for i, timestamp in enumerate(times):
            batch = df.filter(col("derived_ingest_time") == timestamp)
            batch = transform_function(batch, settings, spark)
            if i == 0:
                create_table_if_not_exists(spark, batch, settings["dst_table_name"])
            upsert(batch, i)
            print(f"{table_name}: Upserted batch {i} for time {timestamp}")
        print(f"{table_name}: Rescue completed in {len(times)} batches")

    else:  # mode == "versionAsOf"
        history = spark.sql(f"DESCRIBE HISTORY {settings['src_table_name']}")
        max_version = history.agg({"version": "max"}).first()[0]
        print(f"{table_name}: Max version {max_version}")

        for version in range(0, max_version + 1):
            if version == 0:
                df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
                df = transform_function(df, settings, spark)
                create_table_if_not_exists(spark, df, settings["dst_table_name"])
                print(f"{table_name}: Current version {version}")
                continue

            prev = spark.read.format("delta").option("versionAsOf", version - 1).table(settings["src_table_name"])
            cur = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
            df = cur.subtract(prev)
            df = transform_function(df, settings, spark)
            upsert(df, version - 1)
            print(f"{table_name}: Current version {version}")

        settings["readStreamOptions"]["startingVersion"] = max_version
        Path(settings_path).write_text(json.dumps(settings, indent=4))
        print(f"{table_name}: Updated settings with startingVersion {max_version}")


def rescue_silver_table_timestamp(spark, table_name):
    """Backwards compatible wrapper for ``rescue_silver_table`` using timestamps."""
    rescue_silver_table("timestamp", spark, table_name)


def rescue_silver_table_versionAsOf(spark, table_name):
    """Backwards compatible wrapper for ``rescue_silver_table`` using Delta versions."""
    rescue_silver_table("versionAsOf", spark, table_name)




def rescue_gold_table(spark, table_name):
    """Rebuild a gold table by replaying all versions of the source."""
    settings = json.loads(Path(f"../layer_03_gold/{table_name}.json").read_text())
    settings = apply_job_type(settings)
    settings["ingest_time_column"] = "derived_ingest_time"

    history = spark.sql(f"DESCRIBE HISTORY {settings['src_table_name']}")
    max_version = history.agg({"version": "max"}).first()[0]
    print(f"{table_name}: Max version {max_version}")

    spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    
    transform_function = get_function(settings["transform_function"])
    write_function = get_function(settings["write_function"])

    for version in range(0, max_version + 1):
        df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
        df = transform_function(df, settings, spark)

        if version == 0:
            create_table_if_not_exists(spark, df, settings["dst_table_name"])
        else:
            write_function(df, settings, spark)

        print(f"Current version: {version}")




