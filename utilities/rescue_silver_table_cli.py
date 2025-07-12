#!/usr/bin/env python3
"""Rescue a single silver table."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from functions.rescue import rescue_silver_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Rescue a silver table")
    parser.add_argument("table", help="Table name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = SparkSession.builder.master(args.master).appName("rescue-table").getOrCreate()
    try:
        print(f"Rescuing table: {args.table}")
        rescue_silver_table("timestamp", args.table, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
