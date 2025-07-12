#!/usr/bin/env python3
"""Rescue all silver tables according to schema configuration."""

import argparse
import json
from glob import glob
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from functions.rescue import rescue_silver_table


SKIP = {
    "powerPlay",
    "systemsWithCoordinates7days",
    "systemsWithCoordinates",
    "systemsWithoutCoordinates",
    "bodies7days",
    "codex",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Rescue multiple silver tables")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master(args.master)
        .appName("rescue-schema")
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
        paths = glob("../layer_02_silver/*.json")
        tables = [Path(p).stem for p in paths]
        for table_name in tables:
            if table_name in SKIP:
                continue
            print(f"Rescuing table: {table_name}")
            rescue_silver_table("timestamp", table_name, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
