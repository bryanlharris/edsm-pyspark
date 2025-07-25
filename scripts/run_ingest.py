#!/usr/bin/env python3
"""Run an ingest pipeline using the settings files in this repo.

This script emulates the Databricks notebook ``03_ingest`` so the pipeline can
be executed with a local or cluster spark-submit.
"""

import argparse
import json
from pathlib import Path
import os
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))



from pyspark.sql import SparkSession
from functions.utility import create_spark_session

from functions.utility import (
    get_function,
    create_bad_records_table,
    apply_job_type,
    print_settings,
)

from functions.quality import create_dqx_bad_records_table


def load_settings(color: str, table: str) -> dict:
    """Load and expand the JSON settings for ``table`` in ``color`` layer."""
    path = next(Path().glob(f"./layer_*_{color}/{table}.json"))
    with open(path) as f:
        settings = json.load(f)
    return apply_job_type(settings)


def run_pipeline(color: str, table: str, spark: SparkSession, verbose: bool = False) -> None:
    """Execute the ingest pipeline for the given table."""
    settings = load_settings(color, table)
    dst_table_path = settings.get("dst_table_path")

    if not dst_table_path:
        raise KeyError("dst_table_path must be provided")

    print_settings({"table": table}, settings, color, table, verbose=verbose)

    if "pipeline_function" in settings:
        pipeline_function = get_function(settings["pipeline_function"])
        pipeline_function(settings, spark)
    else:
        read_function = get_function(settings["read_function"])
        transform_function = get_function(settings["transform_function"])
        write_function = get_function(settings["write_function"])

        df = read_function(settings, spark)
        df = transform_function(df, settings, spark)
        df = create_dqx_bad_records_table(df, settings, spark)
        query = write_function(df, settings, spark)
        if hasattr(query, "awaitTermination"):
            query.awaitTermination()

    if color == "bronze":
        create_bad_records_table(settings, spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EDSM ingest pipeline")
    parser.add_argument("color", choices=["bronze", "silver", "gold"], help="Layer color")
    parser.add_argument("table", help="Table name (without .json)")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument("--log-level", default="WARN", help="Spark log level")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print full settings including file schema")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "edsm-ingest", log_level=args.log_level)

    try:
        run_pipeline(args.color, args.table, spark, verbose=args.verbose)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
