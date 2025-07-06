

from pyspark.sql.types import StructType
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def stream_read_cloudfiles(settings, spark):
    """Read streaming data from a cloudFiles source.

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

    # Variables
    readStreamOptions       = settings.get("readStreamOptions")
    readStream_load         = settings.get("readStream_load")
    file_schema             = settings.get("file_schema")

    # Hard code schema is better than inference
    schema = StructType.fromJson(settings["file_schema"])

    # Return a DataFrame
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**readStreamOptions)
        .schema(schema)
        .load(readStream_load)
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










