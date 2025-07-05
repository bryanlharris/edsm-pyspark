#!/usr/bin/env python3
"""Run the ingestion pipeline using a settings file."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from functions.utility import get_function, create_bad_records_table, apply_job_type


def load_settings(color: str, table: str) -> dict:
    pattern = f"./layer_*_{color}/{table}.json"
    path = next(Path().glob(pattern), None)
    if path is None:
        raise FileNotFoundError(f"No settings file matching {pattern}")
    with open(path) as f:
        settings = json.load(f)
    return apply_job_type(settings)


def run_pipeline(spark: SparkSession, color: str, table: str) -> None:
    settings = load_settings(color, table)

    if "pipeline_function" in settings:
        pipeline_fn = get_function(settings["pipeline_function"])
        pipeline_fn(spark, settings)
    elif all(k in settings for k in ["read_function", "transform_function", "write_function"]):
        read_fn = get_function(settings["read_function"])
        transform_fn = get_function(settings["transform_function"])
        write_fn = get_function(settings["write_function"])

        df = read_fn(settings, spark)
        df = transform_fn(df, settings, spark)
        write_fn(df, settings, spark)
    else:
        raise Exception("Could not find any ingest function name in settings.")

    if color == "bronze":
        create_bad_records_table(settings, spark)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingest table using project settings")
    parser.add_argument("--color", required=True, help="Layer color (bronze, silver, gold)")
    parser.add_argument("--table", required=True, help="Table name to ingest")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark, args.color, args.table)


if __name__ == "__main__":
    main()

