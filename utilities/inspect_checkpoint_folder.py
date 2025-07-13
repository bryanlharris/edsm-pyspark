#!/usr/bin/env python3
"""Inspect a Delta checkpoint folder for a given table."""

import argparse
import json
from glob import glob
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import create_spark_session, inspect_checkpoint_folder


def main() -> None:
    paths = glob("../layer_02_silver/*.json")
    table_map = {Path(p).stem: p for p in paths}

    parser = argparse.ArgumentParser(description="Inspect checkpoint directory")
    parser.add_argument("table", choices=table_map.keys(), help="Table name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    settings_path = table_map[args.table]
    settings = json.loads(Path(settings_path).read_text())

    spark = create_spark_session(args.master, "inspect-checkpoints")
    try:
        inspect_checkpoint_folder(settings, args.table, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
