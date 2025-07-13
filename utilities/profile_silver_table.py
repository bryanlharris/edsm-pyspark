#!/usr/bin/env python3
"""Profile a silver table using databricks-labs-dqx."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import create_spark_session, apply_job_type
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from pyspark.sql.functions import col, to_date, when, lit


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile a silver table")
    parser.add_argument("table", help="Table name (without .json)")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "profile-table")
    try:
        settings_path = Path(f"../layer_02_silver/{args.table}.json")
        settings = json.loads(settings_path.read_text())
        settings = apply_job_type(settings)
        dst_table_name = settings["dst_table_name"]

        ws = WorkspaceClient()
        df = spark.table(dst_table_name)
        if "valid_to" in df.columns:
            df = df.withColumn(
                "valid_to",
                when(col("valid_to") > to_date(lit("2100-01-01")), None).otherwise(col("valid_to")),
            )

        profiler = DQProfiler(spark)
        _, profiles = profiler.profile(df)

        generator = DQGenerator(ws)
        checks = generator.generate_dq_rules(profiles, exclude_columns=["_rescued_data"])

        json_str = json.dumps(checks, indent=4, default=str)
        print(json_str)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
