#!/usr/bin/env python3
"""Run the entire EDSM pipeline using PySpark only.

This utility replicates the original Databricks job configuration without
relying on any Databricks APIs. It sequentially executes the downloader script
and then runs the ingest pipeline for each table defined in the layer settings
files.
"""

import argparse
import subprocess
from glob import glob
from pathlib import Path
import sys

# Ensure repo modules are on the path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import create_spark_session

from scripts.run_ingest import run_pipeline
from functions.sanity import (
    validate_settings,
    initialize_empty_tables,
)


def _discover_tables(color: str) -> list[str]:
    """Return a sorted list of table names for ``color`` layer."""
    paths = glob(f"./layer_*_{color}/*.json")
    return sorted(Path(p).stem for p in paths)


def run_job(master: str, verbose: bool = False, log_level: str = "WARN") -> None:
    """Execute the downloader and ingest pipelines."""

    validate_settings()

    spark = create_spark_session(master, "edsm-job", log_level=log_level)
    try:
        initialize_empty_tables(spark)

        downloader = Path(__file__).with_name("downloader.sh")
        subprocess.check_call(["bash", str(downloader)])

        for table in _discover_tables("bronze"):
            run_pipeline("bronze", table, spark, verbose=verbose)

        for table in _discover_tables("silver"):
            run_pipeline("silver", table, spark, verbose=verbose)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full EDSM job locally")
    parser.add_argument(
        "--master", default="local[*]", help="Spark master URL"
    )
    parser.add_argument("--log-level", default="WARN", help="Spark log level")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print full settings including file schema")
    args = parser.parse_args()
    run_job(args.master, verbose=args.verbose, log_level=args.log_level)


if __name__ == "__main__":
    main()
