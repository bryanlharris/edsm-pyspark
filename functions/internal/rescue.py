import json
import subprocess
from pathlib import Path
from pyspark.sql.functions import col
from functions.utility import create_table_if_not_exists, get_function


def rescue_silver_table_timestamp(spark, table_name):
    settings_path = f"../layer_02_silver/{table_name}.json"
    settings = json.loads(Path(settings_path).read_text())

    df = spark.read.table(settings["src_table_name"])
    df = df.orderBy("derived_ingest_time")

    times = [r[0] for r in df.select("derived_ingest_time").distinct().orderBy("derived_ingest_time").collect()]

    transform_function = get_function(settings["transform_function"])
    upsert_function = get_function(settings["upsert_function"])
    upsert = upsert_function(settings, spark)

    spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    checkpointLocation = settings["writeStreamOptions"]["checkpointLocation"]

    if checkpointLocation.startswith("/Volumes/") and checkpointLocation.endswith("_checkpoints/"):
        subprocess.run(["rm", "-rf", checkpointLocation], check=True)
    else:
        raise ValueError(f"Skipping checkpoint deletion, unsupported path: {checkpointLocation}")

    for i, timestamp in enumerate(times):
        batch = df.filter(col("derived_ingest_time") == timestamp)
        batch = transform_function(batch, settings, spark)
        if i == 0:
            create_table_if_not_exists(spark, batch, settings["dst_table_name"])
        upsert(batch, i)
        print(f"{table_name}: Upserted batch {i} for time {timestamp}")

    print(f"{table_name}: Rescue completed in {len(times)} batches")


def rescue_silver_table_versionAsOf(spark, table_name):
    settings_path = f"../layer_02_silver/{table_name}.json"
    settings = json.loads(Path(settings_path).read_text())

    history = spark.sql(f"DESCRIBE HISTORY {settings['src_table_name']}")
    max_version = history.agg({"version":"max"}).first()[0]
    print(f"{table_name}: Max version {max_version}")

    ## Delete old table and checkpointLocation
    spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    checkpointLocation = settings["writeStreamOptions"]["checkpointLocation"]
    if "_checkpoints" not in checkpointLocation:
        raise ValueError(f"Checkpoint location does not contain '_checkpoints': {checkpointLocation}")
    if checkpointLocation.startswith("/Volumes/") and checkpointLocation.endswith("_checkpoints/"):
        subprocess.run(["rm", "-rf", checkpointLocation], check=True)
    else:
        raise ValueError(f"Skipping checkpoint deletion, unsupported path: {checkpointLocation}")

    upsert_function = get_function(settings["upsert_function"])
    transform_function = get_function(settings["transform_function"])

    for version in range(0, max_version+1):
        if version == 0:
            df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
            df = transform_function(df, settings, spark)
            create_table_if_not_exists(spark, df, settings["dst_table_name"])
            print(f"{table_name}: Current version {version}")
            continue

        prev = spark.read.format("delta").option("versionAsOf", version-1).table(settings["src_table_name"])
        cur  = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
        df   = cur.subtract(prev)

        df = transform_function(df, settings, spark)
        upsert(df, version-1)
        print(f"{table_name}: Current version {version}")

    settings["readStreamOptions"]["startingVersion"] = max_version
    Path(settings_path).write_text(json.dumps(settings, indent=4))
    print(f"{table_name}: Updated settings with startingVersion {max_version}")




def rescue_gold_table(spark, table_name):
    settings = json.loads(Path(f"../layer_03_gold/{table_name}.json").read_text())
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




