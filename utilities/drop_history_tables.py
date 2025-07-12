#!/usr/bin/env python3
"""Drop history tables from a schema."""

import argparse
from pyspark.sql import SparkSession


def drop_history_tables(spark: SparkSession, schema: str) -> None:
    tables = spark.sql(f"SHOW TABLES IN {schema}").filter("isTemporary = false")
    table_names = [row.tableName for row in tables.collect()]
    for table in table_names:
        if table.endswith("_file_version_history") or table.endswith("_transaction_history"):
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop history tables from a schema")
    parser.add_argument("schema", help="Schema name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master(args.master)
        .appName("drop-history")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:2.4.0")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    try:
        drop_history_tables(spark, args.schema)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
