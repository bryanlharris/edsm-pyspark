import json
from pyspark.sql.functions import current_timestamp, expr, col
from pyspark.sql.functions import to_timestamp, concat, regexp_extract, lit, date_format
from pyspark.sql.types import StructType, StringType
from functions import create_table_if_not_exists, rename_space_columns


def edsm_bronze_load(spark, settings):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    readStream_load         = settings.get("readStream_load")
    writeStream_format      = settings.get("writeStream_format")
    writeStream_outputMode  = settings.get("writeStream_outputMode")
    file_schema             = settings.get("file_schema")

    # Hard code schema is better than inference
    schema = StructType.fromJson(settings["file_schema"])

    # Read & Transform
    (
        spark.readStream
        .format("cloudFiles")
        .options(**readStreamOptions)
        .schema(schema)
        .load(readStream_load)
        .transform(rename_space_columns)
        .withColumn("source_metadata", expr("_metadata"))
        .withColumn(
            "ingest_time",
            to_timestamp(
                concat(
                    regexp_extract(col("source_metadata.file_path"), "/landing/(\\d{8})/", 1),
                    lit(" "),
                    date_format(current_timestamp(), "HH:mm:ss"),
                ),
                "yyyyMMdd HH:mm:ss",
            ),
        )
        .withColumn("_rescued_data", lit(None).cast(StringType()))
        .writeStream
        .format(writeStream_format)
        .options(**writeStreamOptions)
        .outputMode(writeStream_outputMode)
        .trigger(availableNow=True)
        .table(dst_table_name)
    )







