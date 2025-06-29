

from pyspark.sql.types import StructType
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def stream_read_cloudfiles(spark, settings):
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


def stream_read_table(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    readStreamOptions       = settings.get("readStreamOptions")

    return (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
    )


def read_table(spark, settings):
    return spark.read.table(settings["src_table_name"])


def read_windowed_snapshot(spark, settings):
    window = Window.partitionBy(*business_key).orderBy(col(ingest_time_column).desc())
    return (
        spark.read
        .table(settings["src_table_name"])
        .withColumn("row_num", row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
    )


def read_latest_ingest(spark, settings):
    ingest_time_column = settings["derived_ingest_time"]
    df = spark.read.table(settings["src_table_name"])
    max_time = df.agg({ingest_time_column: "max"}).collect()[0][0]
    return df.filter(df["ingest_time"] == max_time)










