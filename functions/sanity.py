import json
import os
from pathlib import Path
from pyspark.sql.types import StructType
from functions.utility import get_function, apply_job_type
from functions.config import PROJECT_ROOT


def _path_exists(path: str, spark) -> bool:
    """Return ``True`` if ``path`` exists in the configured filesystem."""

    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def _discover_settings_files():
    """Return dictionaries of settings files for each layer."""
    project_root = PROJECT_ROOT
    bronze_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_bronze/*.json")
    }
    silver_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_silver/*.json")
    }
    gold_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_gold/*.json")
    }

    return bronze_files, silver_files, gold_files

def validate_settings(bronze=None, silver=None, gold=None):
    """Ensure all settings files contain required keys before processing.

    Parameters
    ----------
    bronze, silver, gold : optional
        Job parameters for each layer. When omitted, the values are
        loaded from the environment variables ``JOB_SETTINGS_BRONZE``,
        ``JOB_SETTINGS_SILVER`` and ``JOB_SETTINGS_GOLD`` respectively.
    """

    bronze_inputs = bronze or os.environ.get("JOB_SETTINGS_BRONZE")
    silver_inputs = silver or os.environ.get("JOB_SETTINGS_SILVER")
    gold_inputs = gold or os.environ.get("JOB_SETTINGS_GOLD")

    for name, value in [("bronze", bronze_inputs), ("silver", silver_inputs), ("gold", gold_inputs)]:
        if value is not None and isinstance(value, str):
            try:
                json.loads(value)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON for {name} job settings") from exc

    bronze_files, silver_files, gold_files = _discover_settings_files()
    required_keys={
        "bronze":["read_function","transform_function","write_function","dst_table_path","file_schema"],
        "silver":["read_function","transform_function","write_function","src_table_path","dst_table_path"],
        "gold":["read_function","transform_function","write_function","src_table_path","dst_table_path"]
    }


    write_key_requirements = {
        "functions.write.stream_upsert_table": [
            "business_key",
            "surrogate_key",
            "upsert_function",
        ],
        "functions.write.batch_upsert_scd2": [
            "business_key",
            "surrogate_key",
            "upsert_function",
        ],
        "functions.write.write_upsert_snapshot": ["business_key"],
    }

    errs = []

    # Check for required functions
    for layer, files in [("bronze", bronze_files), ("silver", silver_files), ("gold", gold_files)]:
        for tbl, path in files.items():
            settings=json.loads(open(path).read())
            settings = apply_job_type(settings)
            for k in required_keys[layer]:
                if k not in settings:
                    errs.append(f"{path} missing {k}")
            write_fn = settings.get("write_function")
            if write_fn in write_key_requirements:
                for req_key in write_key_requirements[write_fn]:
                    if req_key not in settings:
                        errs.append(f"{path} missing {req_key} for write_function {write_fn}")

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Validate settings check passed.")


def initialize_empty_tables(spark):
    """Create empty Delta tables based on settings definitions."""

    errs = []
    bronze_files, silver_files, gold_files = _discover_settings_files()

    all_tables = set(list(bronze_files.keys()) + list(silver_files.keys()) + list(gold_files.keys()))

    layers=["bronze","silver","gold"]

    ## For each table and each layer, cascade transforms and create table
    for tbl in sorted(all_tables):
        df=None
        skip_table=False
        for layer in layers:
            if layer=="bronze" and tbl not in bronze_files:
                break
            if layer=="silver" and tbl not in silver_files:
                break
            if layer=="gold" and tbl not in gold_files:
                break
            if layer=="bronze":
                path=bronze_files[tbl]
            elif layer=="silver":
                path=silver_files[tbl]
            elif layer=="gold":
                path=gold_files[tbl]
            settings=json.loads(open(path).read())
            settings = apply_job_type(settings)
            if layer=="bronze":
                settings["use_metadata"] = "false"
                if "file_schema" not in settings:
                    errs.append(f"{path} missing file_schema, cannot create table")
                    skip_table=True
                    break
                schema=StructType.fromJson(settings["file_schema"])
                df=spark.createDataFrame([], schema)
            try:
                transform_function = get_function(settings["transform_function"])
            except Exception:
                errs.append(f"{path} missing transform_function for {tbl}, cannot create table")
                skip_table=True
                break
            df = transform_function(df, settings, spark)
            dst_path = settings.get("dst_table_path")
            if not dst_path:
                errs.append(f"{path} missing dst_table_path, cannot create table")
                skip_table = True
                break
            if not _path_exists(dst_path, spark):
                (
                    df.limit(0)
                    .write.format("delta")
                    .option("delta.columnMapping.mode", "name")
                    .mode("overwrite")
                    .save(dst_path)
                )
            tbl_name = Path(dst_path).name
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {tbl_name} USING DELTA LOCATION '{dst_path}'"
            )
        if skip_table:
            continue

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Initialize empty tables check passed.")
