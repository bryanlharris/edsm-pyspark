#!/usr/bin/env python3
"""Optimize and vacuum all tables in a catalog."""

import argparse
from functions.utility import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize and vacuum tables")
    parser.add_argument("--catalog", default="edsm", help="Catalog name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "optimize-vacuum")
    try:
        catalog = args.catalog
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
                full_name = f"{catalog}.{schema}.{table_name}"
                print(f"Optimizing {full_name}")
                spark.sql(f"OPTIMIZE {full_name}")
                print(f"Vacuuming {full_name}")
                spark.sql(f"VACUUM {full_name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
