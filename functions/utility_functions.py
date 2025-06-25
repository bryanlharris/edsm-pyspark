import json
import importlib
from pathlib import Path
from pyspark.sql.types import StructType







def create_table_if_not_exists(spark, df, dst_table_name):
    """Create a table from a dataframe if it doesn't exist"""
    if not spark.catalog.tableExists(dst_table_name):
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(dst_table_name)
        # spark.sql(f"ALTER TABLE {dst_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def create_schema_if_not_exists(spark, catalog, schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def create_volume_if_not_exists(spark, catalog, schema, volume):
    s3_path = f"s3://edsm/volumes/{catalog}/{schema}/{volume}"
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{s3_path}'")


def truncate_table_if_exists(spark, table_name):
    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")
















# def create_tables_for_all_layers(spark):
#     modules = {
#         "bronze": importlib.import_module("layer_01_bronze"),
#         "silver": importlib.import_module("layer_02_silver"),
#         "gold": importlib.import_module("layer_03_gold")
#     }
#     color_to_folder = {
#         "bronze": "layer_01_bronze",
#         "silver": "layer_02_silver",
#         "gold": "layer_03_gold"
#     }
#     colors = ["bronze", "silver", "gold"]

#     all_files = set()
#     for color in colors:
#         folder = Path(color_to_folder[color])
#         for p in folder.glob("*.json"):
#             all_files.add(p.name)

#     for filename in sorted(all_files):
#         df = None
#         for color in colors:
#             settings_path = Path(color_to_folder[color]) / filename
#             if not settings_path.exists():
#                 break
#             with open(settings_path) as f:
#                 settings = json.load(f)
#             required_keys = {"transform_function", "dst_table_name"}
#             if color == "bronze":
#                 required_keys.add("file_schema")
#             if not required_keys.issubset(settings):
#                 break
#             if color == "bronze":
#                 schema = StructType.fromJson(settings["file_schema"])
#                 df = spark.createDataFrame([], schema)
#             modname, funcname = settings["transform_function"].split(".")
#             transform_function = getattr(modules[modname], funcname)
#             df = transform_function(spark, settings, df)
#             dst_table_name = settings["dst_table_name"]
#             create_table_if_not_exists(spark, df, dst_table_name)