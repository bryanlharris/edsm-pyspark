#!/usr/bin/env python3
"""Run an ingest pipeline using the settings files in this repo.

This script emulates the Databricks notebook ``03_ingest`` so the pipeline can
be executed with a local or cluster spark-submit.
"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from functions.utility import (
    get_function,
    create_bad_records_table,
    apply_job_type,
    schema_exists,
    print_settings,
)
from functions.history import build_and_merge_file_history, transaction_history
from functions.quality import create_dqx_bad_records_table


def load_settings(color: str, table: str) -> dict:
    """Load and expand the JSON settings for ``table`` in ``color`` layer."""
    path = next(Path().glob(f"./layer_*_{color}/{table}.json"))
    with open(path) as f:
        settings = json.load(f)
    return apply_job_type(settings)


def run_pipeline(color: str, table: str, spark: SparkSession) -> None:
    """Execute the ingest pipeline for the given table."""
    settings = load_settings(color, table)
    dst_table_name = settings["dst_table_name"]

    print_settings({"table": table}, settings, color, table)

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
        write_function(df, settings, spark)

    if color == "bronze":
        create_bad_records_table(settings, spark)

    history_schema = settings.get("history_schema")
    build_history = str(settings.get("build_history", "false")).lower() == "true"
    if build_history:
        catalog = dst_table_name.split(".")[0]
        if history_schema is None:
            print("Skipping history build: no history_schema provided")
        elif schema_exists(catalog, history_schema, spark):
            build_and_merge_file_history(dst_table_name, history_schema, spark)
            transaction_history(dst_table_name, history_schema, spark)
        else:
            print(
                f"Skipping history build: schema {catalog}.{history_schema} not found"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EDSM ingest pipeline")
    parser.add_argument("color", choices=["bronze", "silver", "gold"], help="Layer color")
    parser.add_argument("table", help="Table name (without .json)")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master(args.master)
        .appName("edsm-ingest")
        .getOrCreate()
    )

    try:
        run_pipeline(args.color, args.table, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
