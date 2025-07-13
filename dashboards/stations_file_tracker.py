#!/usr/bin/env python3
"""Run Stations file tracker query."""

import argparse
from pathlib import Path
from functions.utility import create_spark_session

QUERY = Path(__file__).with_name('sql').joinpath('stations_query.sql').read_text()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stations file tracker")
    parser.add_argument("--id", default="")
    parser.add_argument("--name", default="")
    parser.add_argument("--systemId", default="")
    parser.add_argument("--type", default="")
    parser.add_argument("--faction", default="")
    parser.add_argument("--government", default="")
    parser.add_argument("--allegiance", default="")
    parser.add_argument("--valid-from", dest="valid_from", default="")
    parser.add_argument("--valid-to", dest="valid_to", default="")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "stations-tracker")
    try:
        params = vars(args)
        df = spark.sql(QUERY.format(**params))
        df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
