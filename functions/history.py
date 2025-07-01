import json
from .utility import create_table_if_not_exists
from pyspark.sql.functions import col, lit, expr

def describe_and_filter_history(spark, full_table_name):
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

def build_and_merge_file_history(spark, full_table_name, history_schema):
    catalog, schema, table = full_table_name.split(".")
    file_version_table_name = f"{catalog}.{history_schema}.{table}_file_version_history"
    prev_files = set()
    file_version_history_records = []
    version_list = describe_and_filter_history(spark, full_table_name)

    for version in version_list:
        this_version_df = (
            spark.read
            .format("delta")
            .option("versionAsOf", version)
            .table(full_table_name)
            .select(col("source_metadata.file_path").alias("file_path"))
            .dropDuplicates()
        )
        file_path_list = this_version_df.collect()
        file_path_list = [row.file_path for row in file_path_list]
        new_files = set(file_path_list) - prev_files
        prev_files.update(new_files)
        if len(new_files) > 0:
            file_version_history_records.append((version, list(new_files)))

    if len(file_version_history_records) > 0:
        df = spark.createDataFrame(file_version_history_records, "version LONG, file_path ARRAY<STRING>")
        create_table_if_not_exists(spark, df, file_version_table_name)
        df.createOrReplaceTempView("df")
        spark.sql(f"""
            merge into {file_version_table_name} as target
            using df as source
            on target.version = source.version
            when matched then update set *
            when not matched then insert *
        """)

def transaction_history(spark, full_table_name, history_schema):
    catalog, schema, table = full_table_name.split(".")
    transaction_table_name = f"{catalog}.{history_schema}.{table}_transaction_history"
    df = (
        spark.sql(f"describe history {full_table_name}")
        .withColumn("table_name", lit(full_table_name))
        .selectExpr("table_name", "* except (table_name)")
    )
    create_table_if_not_exists(spark, df, transaction_table_name)
    df.createOrReplaceTempView("df")
    spark.sql(f"""
        merge into {transaction_table_name} as target
        using df as source
        on target.version = source.version
        when matched then update set *
        when not matched then insert *
    """)










