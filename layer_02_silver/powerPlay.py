import json
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp
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

    # Read and transform
    df = (
        spark.readStream
        .format("delta")
        .options(**readStreamOptions)
        .table(src_table_name)
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .withColumn("file_path", col("source_metadata").getField("file_path"))
        .withColumn("file_modification_time", col("source_metadata").getField("file_modification_time"))
        .withColumn("ingest_time", current_timestamp())
        .dropDuplicates()
    )

    # Sanity check
    create_table_if_not_exists(spark, df, dst_table_name)

    # Write
    query = (
        df.writeStream
        .queryName(dst_table_name)
        .format("delta")
        .options(**writeStreamOptions)
        .trigger(availableNow=True)
        .foreachBatch(lambda df, epoch_id: (
            DeltaTable.forName(spark, dst_table_name)
            .alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        ))
        .outputMode("update")
        .start()
    )




