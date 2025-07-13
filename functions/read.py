

import os
import json
import glob

from pyspark.sql.types import StructType
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def list_unseen_dirs(base_path, pattern, checkpoint_sources_path):
    """Return directories containing files not present in checkpoints."""

    read_files = set()
    for path in glob.glob(os.path.join(checkpoint_sources_path, "offset-*")):
        with open(path) as f:
            content = json.load(f)
            for file_path in content.get("logOffset", {}).get("batchFiles", []):
                if file_path.startswith("file:"):
                    read_files.add(file_path.replace("file:", "", 1))

    all_files = glob.glob(os.path.join(base_path, pattern), recursive=True)
    unseen = [f for f in all_files if f not in read_files]
    return sorted(set(os.path.dirname(f) for f in unseen))


def stream_read_files(settings, spark):
    """Read streaming data from a directory of files.

    This function emulates the behaviour of ``stream_read_cloudfiles`` using
    only standard PySpark features so that pipelines can be executed outside of
    Databricks. Options beginning with ``cloudFiles.`` have the prefix removed
    before being passed to the reader and unsupported options are ignored.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session used to construct the reader.
    settings : dict
        Dictionary of options containing ``readStreamOptions`` and
        ``readStream_load`` values as well as the ``file_schema`` definition.

    Returns
    -------
    DataFrame
        A streaming DataFrame configured with the provided schema and options.
    """

    readStreamOptions = settings.get("readStreamOptions", {})
    if "recursiveFileLookup" in readStreamOptions:
        raise ValueError(
            "recursiveFileLookup is not supported; remove this option and use checkpoint-based discovery"
        )
    readStream_load = settings.get("readStream_load")

    # Determine the file format using ``file_format`` or ``format`` options.
    file_format = (
        settings.get("file_format")
        or readStreamOptions.get("format")
        or readStreamOptions.get("cloudFiles.format")
    )
    if not file_format:
        raise ValueError("File format must be specified in settings")

    # Remove databricks specific options and ``cloudFiles.`` prefixes
    options = {}
    for key, value in readStreamOptions.items():
        if key in {"cloudFiles.format"}:
            continue
        if key.startswith("cloudFiles."):
            key = key.split(".", 1)[1]
            # Options like ``schemaLocation`` or ``rescuedDataColumn`` are
            # specific to Auto Loader and ignored when running locally.
            if key in {"schemaLocation", "rescuedDataColumn", "inferColumnTypes"}:
                continue
        options[key] = value

    schema = StructType.fromJson(settings["file_schema"])

    load_args = [readStream_load]
    if "**" in readStream_load:
        idx = readStream_load.index("**")
        base_path = readStream_load[:idx]
        pattern = readStream_load[idx:]
        checkpoint = os.path.join(
            settings["writeStreamOptions"]["checkpointLocation"].rstrip("/"),
            "sources",
            "0",
        )
        dirs = list_unseen_dirs(base_path, pattern, checkpoint)
        if not dirs:
            raise RuntimeError("No new files to process")
        load_args = dirs

    return (
        spark.readStream
        .format(file_format)
        .options(**options)
        .schema(schema)
        .load(*load_args)
    )


def stream_read_table(settings, spark):
    """Create a streaming DataFrame from an existing Delta table."""

    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    readStreamOptions       = settings.get("readStreamOptions")

    return (
        spark.readStream
        .format("delta")
        .options(**readStreamOptions)
        .table(src_table_name)
    )


def read_table(settings, spark):
    """Read a Delta table in batch mode."""
    return spark.read.table(settings["src_table_name"])


def read_snapshot_windowed(settings, spark):
    """Return the most recent record for each key based on an ingest column."""

    surrogate_key           = settings["surrogate_key"]
    ingest_time_column      = settings["ingest_time_column"]
    window = Window.partitionBy(*surrogate_key).orderBy(col(ingest_time_column).desc())
    return (
        spark.read
        .table(settings["src_table_name"])
        .withColumn("row_num", row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
    )


def read_latest_ingest(settings, spark):
    """Load only the records from the latest ingest time."""
    ingest_time_column = settings["ingest_time_column"]
    df = spark.read.table(settings["src_table_name"])
    max_time = df.agg({ingest_time_column: "max"}).collect()[0][0]
    return df.filter(df[ingest_time_column] == max_time)










