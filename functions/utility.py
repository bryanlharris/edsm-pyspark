import os
import json
import importlib
import subprocess
from glob import glob
from pathlib import Path
from pyspark.sql.types import StructType

# Map short ``job_type`` names to ingest function combinations. These names are
# used when ``simple_settings`` is enabled in a settings file.
JOB_TYPE_MAP = {
    "bronze_standard_streaming": {
        "read_function": "functions.read.stream_read_cloudfiles",
        "transform_function": "functions.transform.bronze_standard_transform",
        "write_function": "functions.write.stream_write_table",
    },
    "silver_scd2_streaming": {
        "read_function": "functions.read.stream_read_table",
        "transform_function": "functions.transform.silver_scd2_transform",
        "write_function": "functions.write.stream_upsert_table",
        "upsert_function": "functions.write.microbatch_upsert_scd2_fn",
    },
    "silver_standard_streaming": {
        "read_function": "functions.read.stream_read_table",
        "transform_function": "functions.transform.silver_standard_transform",
        "write_function": "functions.write.stream_upsert_table",
        "upsert_function": "functions.write.microbatch_upsert_fn",
    },
    "silver_scd2_batch": {
        "read_function": "functions.read.read_table",
        "transform_function": "functions.transform.silver_scd2_transform",
        "write_function": "functions.write.batch_upsert_scd2",
    },
    "silver_standard_batch": {
        "read_function": "functions.read.read_table",
        "transform_function": "functions.transform.silver_standard_transform",
        "write_function": "functions.write.write_upsert_snapshot",
    },
}


def _merge_dicts(base, override):
    """Recursively merge two dictionaries.

    ``override`` values take precedence over ``base``. Nested dictionaries are
    merged rather than replaced.
    """

    result = base.copy()
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
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
            if not dst:
                raise KeyError("dst_table_name must be provided for bronze_standard_streaming")
            catalog, schema, table = dst.split(".", 2)
            base_volume = f"/Volumes/{catalog}/{schema}/utility/{table}"
            dynamic = {
                "build_history": "true",
                "readStream_load": f"/Volumes/{catalog}/{schema}/landing/",
                "readStreamOptions": {
                    "cloudFiles.inferColumnTypes": "false",
                    "inferSchema": "false",
                    "cloudFiles.schemaLocation": f"{base_volume}/_schema/",
                    "badRecordsPath": f"{base_volume}/_badRecords/",
                    "cloudFiles.rescuedDataColumn": "_rescued_data",
                },
                "writeStreamOptions": {
                    "mergeSchema": "false",
                    "checkpointLocation": f"{base_volume}/_checkpoints/",
                    "delta.columnMapping.mode": "name",
                },
            }
            defaults = _merge_dicts(defaults, dynamic)
        elif job_type.startswith("silver") or job_type.startswith("gold"):
            dynamic = {"build_history": "false", "ingest_time_column": "ingest_time"}

            if "streaming" in job_type:
                dst = settings.get("dst_table_name")
                if not dst:
                    raise KeyError("dst_table_name must be provided for streaming job types")
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
    ``"functions.read.stream_read_cloudfiles"``.  The string is split
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
    """Create a table from a dataframe if it doesn't exist"""
    if not spark.catalog.tableExists(dst_table_name):
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(dst_table_name)
        return True
    return False


def create_schema_if_not_exists(catalog, schema, spark):
    """Create the schema if it is missing."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def schema_exists(catalog, schema, spark):
    """Return True if the schema exists in the given catalog"""
    df = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'")
    return df.count() > 0


def create_volume_if_not_exists(catalog, schema, volume, spark):
    """Create an external volume pointing to the expected S3 path."""

    s3_path = f"s3://edsm/volumes/{catalog}/{schema}/{volume}"
    spark.sql(
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{s3_path}'"
    )


def truncate_table_if_exists(table_name, spark):
    """Truncate ``table_name`` if it already exists."""

    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")

def inspect_checkpoint_folder(table_name, settings, spark):
    """Print batch to version mapping from a Delta checkpoint folder."""

    checkpoint_path = settings.get("writeStreamOptions", {}).get("checkpointLocation")
    checkpoint_path = checkpoint_path.rstrip("/")
    offsets_path = f"{checkpoint_path}/offsets"

    files = glob(f"{offsets_path}/*")
    sorted_files = sorted(files, key=lambda f: int(Path(f).name))

    print(f"{table_name}: batch → bronze version mapping")

    for path in sorted_files:
        batch_id = Path(path).name
        result = subprocess.run(["grep", "reservoirVersion", path], capture_output=True, text=True)
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
