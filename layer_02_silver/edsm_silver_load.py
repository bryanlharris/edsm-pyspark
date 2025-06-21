import json
from delta.tables import DeltaTable
from pyspark.sql.functions import struct, to_json, sha2, col, current_timestamp
from pyspark.sql.functions import to_timestamp, concat, regexp_extract, lit, date_format
from functions import create_table_if_not_exists, rename_columns, cast_data_types

def edsm_silver_load(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    column_map              = settings.get("column_map")
    data_type_map           = settings.get("data_type_map")

    (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
        .writeStream
        .queryName(dst_table_name)
        .options(**writeStreamOptions)
        .trigger(availableNow=True)
        .foreachBatch(silver_upsert(spark, settings))
        .outputMode("update")
        .start()
    )


def silver_upsert(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    column_map              = settings.get("column_map")
    data_type_map           = settings.get("data_type_map")

    def upsert(microBatchDF, batchId):
        microBatchDF = (
            microBatchDF.transform(rename_columns, column_map)
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

        # Sanity check
        if batchId == 0:
            create_table_if_not_exists(spark, microBatchDF, dst_table_name)

        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
        if business_key:
            change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])
        else:
            change_condition = "FALSE"

        microBatchDF.createOrReplaceTempView("updates")
        spark.sql(
            f"""
            MERGE INTO {dst_table_name} t
            USING updates s
            ON {merge_condition}
            WHEN MATCHED AND ({change_condition}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    return upsert






