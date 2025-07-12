import json
import os
from pyspark.sql.types import StructType
from functions.utility import (
    create_table_if_not_exists,
    get_function,
    apply_job_type,
    create_schema_if_not_exists,
    create_volume_if_not_exists,
    catalog_exists,
)
from functions.config import PROJECT_ROOT


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

def validate_settings(dbutils):
    """Ensure all settings files contain required keys before processing."""

    ## Check that all json settings files have the minimum required keys AKA functions before proceeding
    bronze_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="bronze")
    silver_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="silver")
    gold_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="gold")

    bronze_files, silver_files, gold_files = _discover_settings_files()

    all_tables = set(list(bronze_files.keys()) + list(silver_files.keys()) + list(gold_files.keys()))

    layers=["bronze","silver","gold"]
    required_keys={
        "bronze":["read_function","transform_function","write_function","dst_table_name","file_schema"],
        "silver":["read_function","transform_function","write_function","src_table_name","dst_table_name"],
        "gold":["read_function","transform_function","write_function","src_table_name","dst_table_name"]
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

    # Ensure the destination catalogs match the current host name
    check_host_name_matches_catalog(dbutils)


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
            df=transform_function(df, settings, spark)
            dst=settings["dst_table_name"]
            create_table_if_not_exists(df, dst, spark)
        if skip_table:
            continue

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Initialize empty tables check passed.")


def initialize_schemas_and_volumes(spark):
    """Create schemas and external volumes based on settings definitions."""

    bronze_files, silver_files, gold_files = _discover_settings_files()

    errs = []

    schemas = {"bronze": set(), "silver": set(), "gold": set(), "history": set()}
    file_map = {"bronze": bronze_files, "silver": silver_files, "gold": gold_files}
    catalogs = set()

    for color, files in file_map.items():
        for path in files.values():
            settings = json.loads(open(path).read())
            settings = apply_job_type(settings)
            dst = settings.get("dst_table_name")
            if not dst:
                continue
            catalog, schema, _ = dst.split(".", 2)
            catalogs.add(catalog)
            schemas[color].add((catalog, schema))
            if str(settings.get("build_history", "false")).lower() == "true":
                history_schema = settings.get("history_schema")
                if history_schema:
                    schemas["history"].add((catalog, history_schema))

    for color in ["bronze", "silver", "gold"]:
        if len(schemas[color]) > 1:
            errs.append(
                f"Multiple schemas discovered for {color}: {sorted(schemas[color])}"
            )
    if len(schemas["history"]) > 1:
        errs.append(
            f"Multiple history schemas discovered: {sorted(schemas['history'])}"
        )

    if len(catalogs) > 1:
        errs.append(f"Multiple catalogs discovered: {sorted(catalogs)}")
    elif catalogs:
        catalog = next(iter(catalogs))
        if not catalog_exists(catalog, spark):
            errs.append(f"Catalog does not exist: {catalog}")

    if errs:
        raise RuntimeError("Sanity check failed: " + ", ".join(errs))

    volume_map = {
        "bronze": ["landing", "utility"],
        "silver": ["utility"],
        "gold": ["utility"],
    }

    for color in ["bronze", "silver", "gold", "history"]:
        for catalog, schema in sorted(schemas[color]):
            create_schema_if_not_exists(catalog, schema, spark)
            spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.{schema} TO `account users`")
            for volume in volume_map.get(color, []):
                create_volume_if_not_exists(catalog, schema, volume, spark)

    print("Sanity check: Initialize schemas and volumes check passed.")


from functions.config import ALLOWED_HOST_NAMES


def check_host_name(dbutils=None, spark=None):
    """Validate and return the current host name.

    Parameters
    ----------
    dbutils : optional
        Databricks utility object used to fetch the workspace URL.
    spark : optional
        Unused placeholder for compatibility with previous implementations.

    Returns
    -------
    str
        The short host name.

    Raises
    ------
    RuntimeError
        If the host name cannot be determined or is not allowed.
    """

    host_name = None

    if dbutils is not None:
        try:
            ctx = (
                dbutils.notebook.entry_point
                .getDbutils()
                .notebook()
                .getContext()
            )
            url = ctx.workspaceUrl().get()
            host_name = url.split("//")[-1].split(".")[0]
        except Exception:
            host_name = None

    if host_name is None:
        url = os.environ.get("DATABRICKS_HOST")
        if url:
            host_name = url.split("//")[-1].split(".")[0]

    if not host_name:
        raise RuntimeError("Host name could not be determined")

    host_name = host_name.lower()

    if host_name not in ALLOWED_HOST_NAMES:
        raise RuntimeError(f"Host name '{host_name}' is not allowed")

    print(f"Sanity check: Host name recognized as {host_name}.")
    return host_name


def check_host_name_matches_catalog(dbutils=None, spark=None):
    """Ensure catalog names in settings match the current host name."""

    host_name = check_host_name(dbutils, spark)

    if host_name == "dbc-bde2b6e3-4903":
        print(
            "Sanity check: Host name is exempt from catalog matching; skipping check."
        )
        return host_name

    bronze_files, silver_files, gold_files = _discover_settings_files()
    errs = []
    for path in list(bronze_files.values()) + list(silver_files.values()) + list(
        gold_files.values()
    ):
        settings = json.loads(open(path).read())
        settings = apply_job_type(settings)
        dst = settings.get("dst_table_name")
        if not dst:
            continue
        catalog = dst.split(".")[0]
        if catalog.lower() != host_name.lower():
            errs.append(f"{path} catalog '{catalog}' does not match host '{host_name}'")

    if errs:
        raise RuntimeError("Sanity check failed: " + ", ".join(errs))

    print(
        f"Sanity check: All destination catalogs match host name '{host_name}'."
    )
    return host_name









