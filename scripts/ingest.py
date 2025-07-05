#!/usr/bin/env python3
"""Run the ingestion pipeline using a settings file."""

import argparse
import json
from pathlib import Path
import importlib.resources as resources

from pyspark.sql import SparkSession

from functions.utility import get_function, create_bad_records_table, apply_job_type


LAYER_PACKAGE_MAP = {
    "bronze": "layer_01_bronze",
    "silver": "layer_02_silver",
}


def load_settings(color: str, table: str, project_root: str) -> dict:
    root = Path(project_root)
    pattern = f"layer_*_{color}/{table}.json"
    path = next(root.glob(pattern), None)
    if path and path.is_file():
        with open(path) as f:
            settings = json.load(f)
    else:
        package = LAYER_PACKAGE_MAP.get(color)
        if not package:
            raise FileNotFoundError(f"No settings file matching {pattern}")
        try:
            with resources.open_text(package, f"{table}.json") as f:
                settings = json.load(f)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"No settings file matching {pattern}") from exc
    return apply_job_type(settings)


def run_pipeline(
    spark: SparkSession, color: str, table: str, project_root: str
) -> None:
    settings = load_settings(color, table, project_root)

    job_settings = {"table": table, "project_root": project_root}
    settings_message = f"\n\nDictionary from {color}_settings.json:\n\n"
    settings_message += json.dumps(job_settings, indent=4)
    settings_message += f"\n\nContents of {table}.json:\n\n"
    settings_message += json.dumps(settings, indent=4)
    print(settings_message)

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
    parser.add_argument(
        "--project_root",
        default=".",
        help="Path to the project root containing layer settings",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark, args.color, args.table, args.project_root)


if __name__ == "__main__":
    main()

