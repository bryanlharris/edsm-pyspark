

from pyspark.sql.types import StructType

def stream_read_cloudfiles(spark, settings):
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

    # Return a DataFrame
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**readStreamOptions)
        .schema(schema)
        .load(readStream_load)
    )


def stream_read_table(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    readStreamOptions       = settings.get("readStreamOptions")

    return (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
    )


def read_static_table(spark, settings):
    return spark.read.table(settings["src_table_name"])





