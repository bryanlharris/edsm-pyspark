import json
from pyspark.sql.functions import current_timestamp, expr, col
from pyspark.sql.functions import to_timestamp, concat, regexp_extract, lit, date_format
from functions import create_table_if_not_exists

def powerPlay(spark, settings):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    readStream_load         = settings.get("readStream_load")
    writeStream_format      = settings.get("writeStream_format")
    writeStream_outputMode  = settings.get("writeStream_outputMode")

    # Read & Transform
    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**readStreamOptions)
        .load(readStream_load)
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_metadata", expr("_metadata"))
        .withColumn(
            "ingest_time",
            to_timestamp(
                concat(
                    regexp_extract(col("source_metadata.file_path"), "/landing/(\\d{8})/", 1),
                    lit(" "),
                    date_format(col("ingest_time"), "HH:mm:ss"),
                ),
                "yyyyMMdd HH:mm:ss",
            ),
        )
    )

    # Sanity check
    create_table_if_not_exists(spark, df, dst_table_name)

    # Write
    query = (
        df.writeStream
        .format(writeStream_format)
        .options(**writeStreamOptions)
        .outputMode(writeStream_outputMode)
        .trigger(availableNow=True)
        .table(f"{dst_table_name}")
    )







