

def create_table_if_not_exists(spark, df, dst_table_name):
    """Create a table from a dataframe if it doesn't exist"""
    if not spark.catalog.tableExists(f"{dst_table_name}"):
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(dst_table_name)
        spark.sql(f"ALTER TABLE {dst_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def create_schema_if_not_exists(spark, catalog, schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def create_volume_if_not_exists(spark, catalog, schema, volume):
    s3_path = f"s3://edsm/volumes/{catalog}/{schema}/{volume}"
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{s3_path}'")


def truncate_table_if_exists(spark, table_name):
    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")
