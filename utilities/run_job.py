#!/usr/bin/env python3
"""Run the entire EDSM pipeline using PySpark only.

This utility replicates the Databricks job defined in ``job-definition.yaml``
without using any Databricks APIs. It sequentially executes the downloader
script and then runs the ingest pipeline for each table defined in the layer
settings files.
"""

import argparse
import subprocess
from glob import glob
from pathlib import Path
import sys

# Ensure repo modules are on the path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from scripts.run_ingest import run_pipeline
from functions.sanity import (
    validate_settings,
    initialize_schemas_and_volumes,
    initialize_empty_tables,
)


def _discover_tables(color: str) -> list[str]:
    """Return a sorted list of table names for ``color`` layer."""
    paths = glob(f"./layer_*_{color}/*.json")
    return sorted(Path(p).stem for p in paths)


def run_job(master: str) -> None:
    """Execute the downloader and ingest pipelines."""

    validate_settings()

    spark = (
        SparkSession.builder.master(master)
        .appName("edsm-job")
        .getOrCreate()
    )
    try:
        initialize_schemas_and_volumes(spark)
        initialize_empty_tables(spark)

        downloader = Path(__file__).with_name("downloader.sh")
        subprocess.check_call(["bash", str(downloader)])

        for table in _discover_tables("bronze"):
            run_pipeline("bronze", table, spark)

        for table in _discover_tables("silver"):
            run_pipeline("silver", table, spark)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full EDSM job locally")
    parser.add_argument(
        "--master", default="local[*]", help="Spark master URL"
    )
    args = parser.parse_args()
    run_job(args.master)


if __name__ == "__main__":
    main()
