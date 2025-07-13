import os
import json
import importlib
import subprocess
import html
from glob import glob
from pathlib import Path
from pyspark.sql.types import StructType

from .config import JOB_TYPE_MAP, S3_ROOT_LANDING, S3_ROOT_UTILITY


def create_spark_session(master: str, app_name: str):
    """Return a Spark session configured for Delta."""

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()


def print_settings(job_settings, settings, color, table, verbose=False):
    """Print job and table settings in plain text."""

    print(f"Dictionary from {color}_settings.json:")
    print(json.dumps(job_settings, indent=4))

    print(f"Derived contents of {table}.json:")
    if not verbose and "file_schema" in settings:
        settings = {k: v for k, v in settings.items() if k != "file_schema"}
    print(json.dumps(settings, indent=4))


def _merge_dicts(base, override):
    """Recursively merge two dictionaries.

    ``override`` values take precedence over ``base``. Nested dictionaries are
    merged rather than replaced.
    """

    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def apply_job_type(settings):
    """Expand ``job_type`` into explicit function names.

    When ``settings`` includes ``simple_settings`` set to ``true`` (case
    insensitive), the corresponding functions from ``JOB_TYPE_MAP`` are merged
    into the settings dictionary. Existing keys in ``settings`` take precedence.
    For the ``bronze_standard_streaming`` job type, additional defaults derived
    from ``dst_table_name`` are merged in, and nested dictionaries are combined.
    """

    if str(settings.get("simple_settings", "false")).lower() == "true":
        job_type = settings.get("job_type")
        if not job_type:
            raise KeyError("job_type must be provided when simple_settings is true")
        try:
            defaults = JOB_TYPE_MAP[job_type]
        except KeyError as exc:
            raise KeyError(f"Unknown job_type: {job_type}") from exc

        if job_type == "bronze_standard_streaming":
            dst = settings.get("dst_table_name")
            dst_path = settings.get("dst_table_path")
            if not dst and not dst_path:
                raise KeyError(
                    "dst_table_name or dst_table_path must be provided for bronze_standard_streaming"
                )
            if dst:
                catalog, schema, table = dst.split(".", 2)
                base_volume = f"/Volumes/{catalog}/{schema}/utility/{table}"
                read_load = f"/Volumes/{catalog}/{schema}/landing/"
            else:
                p = Path(dst_path)
                table = p.name
                root = str(p.parent)
                base_volume = f"{root}/utility/{table}"
                read_load = f"{root}/landing/"
            dynamic = {
                "readStream_load": read_load,
                "readStreamOptions": {
                    "inferSchema": "false",
                    "badRecordsPath": f"{base_volume}/_badRecords/",
                },
                "writeStreamOptions": {
                    "mergeSchema": "false",
                    "checkpointLocation": f"{base_volume}/_checkpoints/",
                    "delta.columnMapping.mode": "name",
                },
            }
            defaults = _merge_dicts(defaults, dynamic)
        elif job_type.startswith("silver") or job_type.startswith("gold"):
            dynamic = {"ingest_time_column": "ingest_time"}

            if "streaming" in job_type:
                dst = settings.get("dst_table_name")
                if not dst:
                    raise KeyError(
                        "dst_table_name must be provided for streaming job types"
                    )
                catalog, color, table = dst.split(".", 2)
                base_volume = f"/Volumes/{catalog}/{color}/utility/{table}"
                stream_defaults = {
                    "readStreamOptions": {},
                    "writeStreamOptions": {
                        "mergeSchema": "false",
                        "checkpointLocation": f"{base_volume}/_checkpoints/",
                        "delta.columnMapping.mode": "name",
                    },
                }
                dynamic = _merge_dicts(dynamic, stream_defaults)

            defaults = _merge_dicts(defaults, dynamic)

        settings = _merge_dicts(defaults, settings)



    return settings


def get_function(path):
    """Return a callable from a dotted module path.

    ``path`` should be a fully qualified object name such as
    ``"functions.read.stream_read_files"``.  The string is split
    once from the right with ``path.rsplit(".", 1)`` so any dots before
    the last one become part of the module path and the final segment is
    the attribute name.  A ``ValueError`` is raised if no ``'.'`` is present.
    ``ModuleNotFoundError`` or ``AttributeError`` propagate when the
    module or attribute cannot be imported.
    """

    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def create_table_if_not_exists(df, dst_table_name, spark):
    """Create a table from a dataframe if it doesn't exist.

    Returns
    -------
    bool
        ``True`` if the table was created, ``False`` otherwise.
    """

    if not spark.catalog.tableExists(dst_table_name):
        empty_df = spark.createDataFrame([], df.schema)
        (
            empty_df.write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .saveAsTable(dst_table_name)
        )
        print(f"\tINFO: Table did not exist and was created: {dst_table_name}.")
        return True

    return False


def create_schema_if_not_exists(catalog, schema, spark):
    """Create the schema if it is missing and print a message."""

    if not schema_exists(catalog, schema, spark):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        print(f"\tINFO: Schema did not exist and was created: {catalog}.{schema}.")


def schema_exists(catalog, schema, spark):
    """Return ``True`` if ``schema`` exists in ``catalog``.

    Parameters
    ----------
    catalog : str
        Name of the catalog to inspect.
    schema : str
        Schema name to check for existence.
    spark : pyspark.sql.SparkSession
        Active Spark session used to query the metastore.

    Returns
    -------
    bool
        ``True`` when the schema is present, ``False`` otherwise.
    """
    df = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'")
    return df.count() > 0


def catalog_exists(catalog, spark):
    """Return ``True`` if ``catalog`` exists in the metastore.

    Parameters
    ----------
    catalog : str
        Catalog name to check.
    spark : pyspark.sql.SparkSession
        Active Spark session used to query the metastore.

    Returns
    -------
    bool
        ``True`` when the catalog exists, ``False`` otherwise.
    """
    df = spark.sql(f"SHOW CATALOGS LIKE '{catalog}'")
    return df.count() > 0





def truncate_table_if_exists(table_name, spark):
    """Truncate ``table_name`` if it already exists."""

    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")


def inspect_checkpoint_folder(table_name, settings, spark):
    """Print batch to version mapping from a Delta checkpoint folder."""
    checkpoint_path = settings.get("writeStreamOptions", {}).get("checkpointLocation")
    if not checkpoint_path:
        raise ValueError(
            "Missing 'writeStreamOptions.checkpointLocation' in the settings file"
        )
    checkpoint_path = checkpoint_path.rstrip("/")
    offsets_path = f"{checkpoint_path}/offsets"

    files = glob(f"{offsets_path}/*")
    sorted_files = sorted(files, key=lambda f: int(Path(f).name))

    print(f"{table_name}: batch → bronze version mapping")

    for path in sorted_files:
        batch_id = Path(path).name
        result = subprocess.run(
            ["grep", "reservoirVersion", path], capture_output=True, text=True
        )
        version = json.loads(result.stdout)["reservoirVersion"]
        print(f"  Silver Batch {batch_id} → Bronze version {version - 1}")


def create_bad_records_table(settings, spark):
    """Create a delta table from the JSON files located in ``badRecordsPath``.

    If the path does not exist, any existing table ``<dst_table_name>_bad_records``
    is dropped.  If the table exists after this function runs, an exception is
    raised to signal that bad records were found.
    """

    dst_table_name = settings.get("dst_table_name")
    bad_records_path = settings.get("readStreamOptions", {}).get("badRecordsPath")

    if not dst_table_name or not bad_records_path:
        return

    try:
        path = Path(bad_records_path)
        if path.is_dir() and any(path.iterdir()):
            df = spark.read.json(bad_records_path)
            df.write.mode("overwrite").format("delta").saveAsTable(
                f"{dst_table_name}_bad_records"
            )
        else:
            raise FileNotFoundError
    except Exception:
        spark.sql(f"DROP TABLE IF EXISTS {dst_table_name}_bad_records")

    if spark.catalog.tableExists(f"{dst_table_name}_bad_records"):
        raise Exception(f"Bad records table exists: {dst_table_name}_bad_records")
