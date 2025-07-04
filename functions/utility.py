import os
import json
import importlib
import subprocess
from glob import glob
from pathlib import Path
from pyspark.sql.types import StructType


def get_function(path):
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def create_table_if_not_exists(spark, df, dst_table_name):
    """Create a table from a dataframe if it doesn't exist"""
    if not spark.catalog.tableExists(dst_table_name):
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(dst_table_name)
        return True
    return False


def create_schema_if_not_exists(spark, catalog, schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def schema_exists(spark, catalog, schema):
    """Return True if the schema exists in the given catalog"""
    df = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'")
    return df.count() > 0


def create_volume_if_not_exists(spark, catalog, schema, volume):
    s3_path = f"s3://edsm/volumes/{catalog}/{schema}/{volume}"
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{s3_path}'")


def truncate_table_if_exists(spark, table_name):
    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")

def inspect_checkpoint_folder(settings, table_name, spark):
    checkpoint_path = settings.get("writeStreamOptions", {}).get("checkpointLocation")
    checkpoint_path = checkpoint_path.rstrip("/")
    offsets_path = f"{checkpoint_path}/offsets"

    files = glob(f"{offsets_path}/*")
    sorted_files = sorted(files, key=lambda f: int(Path(f).name))

    print(f"{table_name}: batch → bronze version mapping")

    for path in sorted_files:
        batch_id = Path(path).name
        result = subprocess.run(["grep", "reservoirVersion", path], capture_output=True, text=True)
        version = json.loads(result.stdout)["reservoirVersion"]
        print(f"  Silver Batch {batch_id} → Bronze version {version - 1}")


def create_bad_records_table(spark, settings, dbutils):
    """Create a delta table from the JSON files located in ``badRecordsPath``.

    If the path does not exist, any existing table ``<dst_table_name>_bad_records``
    is dropped.  If the table exists after this function runs, an exception is
    raised to signal that bad records were found.
    """

    dst_table_name = settings.get("dst_table_name")
    bad_records_path = settings.get("readStreamOptions", {}).get("badRecordsPath")

    if not dst_table_name or not bad_records_path:
        return

    try:
        dbutils.fs.ls(bad_records_path)
        df = spark.read.json(bad_records_path)
        df.write.mode("overwrite").format("delta").saveAsTable(f"{dst_table_name}_bad_records")
    except Exception:
        spark.sql(f"DROP TABLE IF EXISTS {dst_table_name}_bad_records")

    if spark.catalog.tableExists(f"{dst_table_name}_bad_records"):
        raise Exception(f"Bad records table exists: {dst_table_name}_bad_records")
