import argparse
import json
from functions.utility import (
    get_function,
    create_bad_records_table,
    apply_job_type,
)
from functions.project_root import PROJECT_ROOT
from pyspark.sql import SparkSession


def _load_settings(color: str, table: str) -> dict:
    pattern = f"layer_*_{color}/{table}.json"
    matches = list(PROJECT_ROOT.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No settings file matching {pattern}")
    path = matches[0]
    with open(path, "r", encoding="utf-8") as fh:
        settings = json.load(fh)
    return apply_job_type(settings)


def _run_pipeline(settings: dict, spark: SparkSession, color: str) -> None:
    if "pipeline_function" in settings:
        pipeline_function = get_function(settings["pipeline_function"])
        pipeline_function(settings, spark)
    elif all(k in settings for k in ["read_function", "transform_function", "write_function"]):
        read_function = get_function(settings["read_function"])
        transform_function = get_function(settings["transform_function"])
        write_function = get_function(settings["write_function"])

        df = read_function(settings, spark)
        df = transform_function(df, settings, spark)
        write_function(df, settings, spark)
    else:
        raise Exception("Could not find any ingest function name in settings.")

    if color == "bronze":
        create_bad_records_table(settings, spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute a table ingest pipeline")
    parser.add_argument(
        "--color",
        required=True,
        help="Dataset layer color, e.g. bronze or silver",
    )
    parser.add_argument(
        "--table", required=True, help="Table name defined by the settings file"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    settings = _load_settings(args.color, args.table)
    _run_pipeline(settings, spark, args.color)


if __name__ == "__main__":
    main()
