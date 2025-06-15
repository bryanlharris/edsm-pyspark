

def create_table_if_not_exists(spark, df, dst_table_name):
    """Create a table from a dataframe if it doesn't exist"""
    if not spark.catalog.tableExists(f"{dst_table_name}"):
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(dst_table_name)


