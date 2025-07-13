#!/usr/bin/env python3
"""Run PowerPlay file tracker queries."""

import argparse
from pathlib import Path
from functions.utility import create_spark_session

QUERY = Path(__file__).with_name('sql').joinpath('powerplay_query1.sql').read_text()


def main() -> None:
    parser = argparse.ArgumentParser(description="PowerPlay file tracker")
    parser.add_argument("--id", default="")
    parser.add_argument("--name", default="")
    parser.add_argument("--power", default="")
    parser.add_argument("--allegiance", default="")
    parser.add_argument("--government", default="")
    parser.add_argument("--powerState", default="")
    parser.add_argument("--state", default="")
    parser.add_argument("--valid-from", dest="valid_from", default="")
    parser.add_argument("--valid-to", dest="valid_to", default="")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "powerplay-tracker")
    try:
        params = vars(args)
        df = spark.sql(QUERY.format(**params))
        df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
