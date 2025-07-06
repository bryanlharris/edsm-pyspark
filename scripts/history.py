import argparse
import json
from pyspark.sql import SparkSession
from functions.history import build_and_merge_file_history, transaction_history
from functions.utility import schema_exists


def main():
    parser = argparse.ArgumentParser(
        description="Build file and transaction history for a Delta table",
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="JSON string with at least 'full_table_name' and optional 'history_schema'",
    )
    args = parser.parse_args()

    job_settings = json.loads(args.input)
    full_table_name = job_settings["full_table_name"]
    history_schema = job_settings.get("history_schema")
    catalog = full_table_name.split(".")[0]

    settings_message = "\n\nDictionary dynamically generated from input:\n\n"
    settings_message += json.dumps(job_settings, indent=4)
    print(settings_message)

    spark = SparkSession.builder.getOrCreate()

    if history_schema is None:
        print("Skipping history build: no history_schema provided")
    elif schema_exists(catalog, history_schema, spark):
        build_and_merge_file_history(full_table_name, history_schema, spark)
        transaction_history(full_table_name, history_schema, spark)
    else:
        print(f"Skipping history build: schema {catalog}.{history_schema} not found")


if __name__ == "__main__":
    main()
