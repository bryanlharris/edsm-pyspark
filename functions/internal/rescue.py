


import json
import subprocess
from pathlib import Path
import layer_01_bronze as bronze
import layer_02_silver as silver
import layer_03_gold as gold
import functions
from functions import create_table_if_not_exists

modules = {
    "functions": functions,
    "bronze": bronze,
    "silver": silver,
    "gold": gold
}

def rescue_silver_table(spark, table_name):
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

    ## Need to do this to all silver json settings, will do later
    modname, funcname = settings["upsert_function"].split(".")
    upsert_function = getattr(modules[modname], funcname)
    upsert = upsert_function(spark, settings)

    modname, funcname = settings["transform_function"].split(".")
    transform_function = getattr(modules[modname], funcname)

    for version in range(0, max_version+1):
        if version == 0:
            df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
            df = transform_function(spark, settings, df)
            create_table_if_not_exists(spark, df, settings["dst_table_name"])
            print(f"{table_name}: Current version {version}")
            continue

        prev = spark.read.format("delta").option("versionAsOf", version-1).table(settings["src_table_name"])
        cur  = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
        df   = cur.subtract(prev)

        df = transform_function(spark, settings, df)
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

    modname, funcname = settings["transform_function"].split(".")
    transform_function = getattr(modules[modname], funcname)

    modname, funcname = settings["write_function"].split(".")
    write_function = getattr(modules[modname], funcname)

    for version in range(0, max_version + 1):
        df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
        df = transform_function(spark, settings, df)

        if version == 0:
            create_table_if_not_exists(spark, df, settings["dst_table_name"])
        else:
            write_function(spark, settings, df)

        print(f"Current version: {version}")




