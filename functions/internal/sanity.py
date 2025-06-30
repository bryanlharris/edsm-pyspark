import os
import ast
import json
import importlib
from glob import glob
from pathlib import Path
from pyspark.sql.types import StructType
from functions import create_table_if_not_exists, get_function

## Rebuild __init__.py files before getting started
def extract_function_names(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=filepath)
    return [node.name for node in tree.body if isinstance(node, ast.FunctionDef)]

def generate_init_file(directory):
    lines = []
    all_exports = []
    for filename in sorted(os.listdir(directory)):
        fullpath = os.path.join(directory, filename)
        if filename.endswith(".py") and filename != "__init__.py" and os.path.isfile(fullpath):
            modname = filename[:-3]
            func_names = extract_function_names(fullpath)
            if func_names:
                lines.append(f"from .{modname} import " + ", ".join(func_names))
                all_exports.extend(func_names)
    if all_exports:
        lines.append("")
        lines.append("__all__ = [" + ", ".join(f'"{name}"' for name in all_exports) + "]")
    init_path = os.path.join(directory, "__init__.py")
    with open(init_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

def build_all_init(project_root):
    os.chdir(project_root)
    errs = []
    for layer in [ "functions" ]:
        try:
            generate_init_file(layer)
        except Exception as e:
            errs.append(f"Failed to generate __init__.py for {layer}: {e}")
    if errs:
        raise RuntimeError("Errors in build_all_init: " + "; ".join(errs))
    print("Sanity check: Python __init__.py files rebuild check passed.")


def validate_settings(project_root, dbutils):
    os.chdir(project_root)
    ## Check that all json settings files have the minimum required keys AKA functions before proceeding
    bronze_inputs=dbutils.jobs.taskValues.get(taskKey="job_settings",key="bronze")
    silver_inputs=dbutils.jobs.taskValues.get(taskKey="job_settings",key="silver")
    gold_inputs=dbutils.jobs.taskValues.get(taskKey="job_settings",key="gold")

    bronze_files={f.split("/")[-1].replace(".json",""): f for f in glob("./layer_*_bronze/*.json")}
    silver_files={f.split("/")[-1].replace(".json",""): f for f in glob("./layer_*_silver/*.json")}
    gold_files={f.split("/")[-1].replace(".json",""): f for f in glob("./layer_*_gold/*.json")}

    all_tables = set(list(bronze_files.keys()) + list(silver_files.keys()) + list(gold_files.keys()))

    layers=["bronze","silver","gold"]
    required_functions={
        "bronze":["read_function","transform_function","write_function","dst_table_name","file_schema"],
        "silver":["read_function","transform_function","write_function","src_table_name","dst_table_name","business_key","surrogate_key"],
        "gold":["read_function","transform_function","write_function","src_table_name","dst_table_name","business_key","surrogate_key"]
    }

    optional_functions={
        "silver": ["upsert_function"]
    }

    errs = []

    # Check for required functions
    for layer, files in [("bronze", bronze_files), ("silver", silver_files), ("gold", gold_files)]:
        for tbl, path in files.items():
            settings=json.loads(open(path).read())
            for k in required_functions[layer]:
                if k not in settings:
                    errs.append(f"{path} missing {k}")
            for k in optional_functions[layer]:
                if k in settings:
                    print(f"Found optional function in {path}: {k}")

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Validate settings check passed.")


def initialize_empty_tables(project_root, spark):
    errs = []
    os.chdir(project_root)
    bronze_files={f.split("/")[-1].replace(".json",""): f for f in glob("./layer_*_bronze/*.json")}
    silver_files={f.split("/")[-1].replace(".json",""): f for f in glob("./layer_*_silver/*.json")}
    gold_files={f.split("/")[-1].replace(".json",""): f for f in glob("./layer_*_gold/*.json")}

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
            df=transform_function(df, settings, spark)
            dst=settings["dst_table_name"]
            if create_table_if_not_exists(spark, df, dst):
                print(f"\tINFO: Table did not exist and was created: {dst}.")
        if skip_table:
            continue

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Initialize empty tables check passed.")








