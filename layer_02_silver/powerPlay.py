import json
from delta.tables import DeltaTable
from pyspark.sql.functions import struct, to_json, sha2, col, current_timestamp
from functions import create_table_if_not_exists, rename_columns, cast_data_types

def powerPlay(spark, settings):
    # Variables (json file)
    src_table_name              = settings.get("src_table_name")
    dst_table_name              = settings.get("dst_table_name")
    readStreamOptions           = settings.get("readStreamOptions")
    writeStreamOptions          = settings.get("writeStreamOptions")
    pk_name                     = settings.get("pk", {}).get("name")
    pk_columns                  = settings.get("pk", {}).get("columns")
    merge_condition             = settings.get("merge_condition")
    column_map                  = settings.get("column_map")
    data_type_map               = settings.get("data_type_map")

    def upsert_to_silver(microBatchDF, batchId):
        # Sanity check
        create_table_if_not_exists(spark, microBatchDF, dst_table_name)

        microBatchDF.createOrReplaceTempView("updates")
        spark.sql(
            f"""
            MERGE INTO {dst_table_name} t
            USING updates s
            ON {merge_condition}
            WHEN MATCHED AND t.row_hash<>s.row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    df = (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .withColumn("ingest_time", current_timestamp())
        .withColumn("file_path", col("source_metadata").getField("file_path"))
        .withColumn("file_modification_time", col("source_metadata").getField("file_modification_time"))
    )

    ignore_cols = ( "date", "ingest_time", "file_path", "file_modification_time", "source_metadata" )
    df = df.withColumn(
            "row_hash",
            sha2(to_json(struct(*[col(c) for c in df.columns if c not in ignore_cols])),256)
        )

    (
        df.writeStream
        .queryName(dst_table_name)
        .options(**writeStreamOptions)
        .trigger(availableNow=True)
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .start()
    )










