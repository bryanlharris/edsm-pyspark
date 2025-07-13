#!/usr/bin/env python3
"""Build history tables for a bronze ingest configuration."""
import argparse
import json
from glob import glob
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import (
    create_spark_session,
    schema_exists,
    apply_job_type,
)
from functions.history import transaction_history


def main() -> None:
    paths = glob("../layer_01_bronze/*.json")
    table_map = {Path(p).stem: p for p in paths}
    parser = argparse.ArgumentParser(description="Build file and transaction history")
    parser.add_argument("table", choices=table_map.keys(), help="Table name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    settings_path = table_map[args.table]
    with open(settings_path) as f:
        job_settings = json.load(f)

    full_table_name = job_settings["dst_table_name"]
    history_schema = job_settings.get("history_schema")
    catalog = full_table_name.split(".")[0]

    spark = create_spark_session(args.master, "history")
    try:
        if history_schema is None:
            print("Skipping history build: no history_schema provided")
        elif schema_exists(catalog, history_schema, spark):
            transaction_history(full_table_name, history_schema, spark)
        else:
            print(f"Skipping history build: schema {catalog}.{history_schema} not found")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
