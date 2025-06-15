

import json
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws, coalesce, lit, expr
from functions import *




def bronze_function(spark, settings):
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
        .format("cloudfiles")
        .options(**readStreamOptions)
        .load(readStream_load)
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_metadata", expr("_metadata"))
    )

    # Perform table sanity check
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





def upsert_using_checksum(spark, settings):

    # Variables (json file)
    src_table_name            = settings.get("src_table_name")
    dst_table_name            = settings.get("dst_table_name")
    readStreamOptions         = settings.get("readStreamOptions")
    writeStreamOptions        = settings.get("writeStreamOptions")
    pk_name                   = settings.get("pk", {}).get("name")
    pk_columns                = settings.get("pk", {}).get("columns")
    column_map                = settings.get("column_map")
    data_type_map             = settings.get("data_type_map")

    # Read
    df = (
        spark.readStream
        .format("delta")
        .options(**readStreamOptions)
        .table(src_table_name)
    )

    # Transform
    df = (
        df.transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .withColumn("file_path", col("source_metadata").getField("file_path"))
        .withColumn("file_modification_time", col("source_metadata").getField("file_modification_time"))
        .withColumn("ingest_time", current_timestamp())
        .dropDuplicates()
        .transform(sha_key, pk_name, pk_columns)
    )


    # Perform table sanity check
    create_table_if_not_exists(spark, df, dst_table_name)

    # Write
    query = (
        df.writeStream
        .queryName(dst_table_name)
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(lambda microBatchDF, batchId: DeltaTable.forName(spark, dst_table_name)
            .alias("d")
            .merge(microBatchDF.alias("s"), f"s.{pk_name} = d.{pk_name}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        .start()
    )





def upsert_delete_using_checksum(spark, settings):

    # Variables (json file)
    src_table_name            = settings.get("src_table_name")
    dst_table_name            = settings.get("dst_table_name")
    readStreamOptions         = settings.get("readStreamOptions")
    writeStreamOptions        = settings.get("writeStreamOptions")
    pk_name                   = settings.get("pk", {}).get("name")
    pk_columns                = settings.get("pk", {}).get("columns")
    column_map                = settings.get("column_map")
    data_type_map             = settings.get("data_type_map")


    # Read
    df = (
        spark.readStream
        .format("delta")
        .options(**readStreamOptions)
        .table(src_table_name)
    )

    # Transform
    df = (
        df.transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .withColumn("file_path", col("source_metadata").getField("file_path"))
        .withColumn("file_modification_time", col("source_metadata").getField("file_modification_time"))
        .withColumn("ingest_time", current_timestamp())
        .dropDuplicates()
        .transform(sha_key, pk_name, pk_columns)
    )

    # Perform table sanity check
    create_table_if_not_exists(spark, df, dst_table_name)

    # Write
    query = (
        df.writeStream
        .queryName(dst_table_name)
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(lambda microBatchDF, batchId: DeltaTable.forName(spark, dst_table_name)
            .alias("d")
            .merge(microBatchDF.alias("s"),f"s.{pk_name} = d.{pk_name}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
        .start()
    )









