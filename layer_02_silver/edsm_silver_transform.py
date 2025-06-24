from pyspark.sql.functions import col, concat, lit, regexp_extract, to_timestamp, date_format, current_timestamp
from functions import rename_columns, cast_data_types

def edsm_silver_transform(spark, settings, df):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    column_map              = settings.get("column_map")
    data_type_map           = settings.get("data_type_map")

    return (
        df.transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .withColumn("file_path", col("source_metadata").getField("file_path"))
        .withColumn("file_modification_time", col("source_metadata").getField("file_modification_time"))
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
    )






