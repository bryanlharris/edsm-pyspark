import json
from .utility import create_table_if_not_exists, truncate_table_if_exists
from pyspark.sql.functions import col, lit

def describe_and_filter_history(full_table_name, spark):
    """Return ordered versions that correspond to streaming updates or merges."""

    hist = spark.sql(f"describe history {full_table_name}")
    update_or_merge_version_rows = (
        hist.filter((col("operation") == "STREAMING UPDATE") | (col("operation") == "MERGE"))
        .select("version")
        .distinct()
        .collect()
    )
    version_list = [row["version"] for row in update_or_merge_version_rows]
    version_list = sorted(version_list)
    return version_list

def transaction_history(full_table_name, history_schema, spark):
    """Record the Delta transaction history for ``full_table_name``."""

    catalog, schema, table = full_table_name.split(".")
    transaction_table_name = f"{catalog}.{history_schema}.{table}_transaction_history"
    df = (
        spark.sql(f"describe history {full_table_name}")
        .withColumn("table_name", lit(full_table_name))
        .selectExpr("table_name", "* except (table_name)")
    )
    create_table_if_not_exists(df, transaction_table_name, spark)
    df.createOrReplaceTempView("df")
    spark.sql(f"""
        merge into {transaction_table_name} as target
        using df as source
        on target.version = source.version
        when matched then update set *
        when not matched then insert *
    """)










