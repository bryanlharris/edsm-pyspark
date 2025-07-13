#!/usr/bin/env python3
"""Drop all tables from a catalog and clean utility volumes."""

import argparse
import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import create_spark_session


def remove_volume(path: Path) -> None:
    if path.exists():
        for child in path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


def drop_tables(spark: SparkSession, catalog: str) -> None:
    schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
    for record in schemas:
        schema = record.databaseName
        if schema in ["default", "information_schema", "pg_catalog"]:
            continue
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        for tbl in tables:
            if tbl.isTemporary:
                continue
            table_name = tbl.tableName
            desc = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table_name}").collect()
            is_view = any("Type" in row.col_name and "VIEW" in row.data_type for row in desc)
            if is_view:
                continue
            print(f"Dropping {catalog}.{schema}.{table_name}")
            spark.sql(f"DROP TABLE {catalog}.{schema}.{table_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset catalog")
    parser.add_argument("catalog", help="Catalog name")
    parser.add_argument(
        "--volume-root",
        type=Path,
        default=Path("/Volumes/edsm"),
        help="Root path for volumes",
    )
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    for layer in ["bronze", "silver", "gold"]:
        remove_volume(args.volume_root / layer / "utility")

    spark = create_spark_session(args.master, "reset-catalog")
    try:
        drop_tables(spark, args.catalog)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
