#!/usr/bin/env python3
"""Command line interface for the ``03_ingest`` notebook.

This script provides the logic from ``03_ingest.ipynb`` without using
Databricks widgets. Use it to run an ingest pipeline locally with
``spark-submit`` or directly from the command line.
"""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import create_spark_session

from scripts.run_ingest import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EDSM ingest pipeline")
    parser.add_argument(
        "color", choices=["bronze", "silver", "gold"], help="Layer color"
    )
    parser.add_argument("table", help="Table name (without .json)")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "edsm-ingest")

    try:
        run_pipeline(args.color, args.table, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
