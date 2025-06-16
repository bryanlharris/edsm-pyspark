import json
from delta.tables import DeltaTable
# from pyspark.sql.functions import current_timestamp, isnull, lit
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit
from pyspark.sql.functions import struct, to_json, sha2, col, current_timestamp
from functions import create_table_if_not_exists

def powerPlay(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    merge_condition         = settings.get("merge_condition")

    def upsert_to_gold(microBatchDF, batchId):
        cols = [c for c in microBatchDF.columns if c not in ("ingest_time")]
        microBatchDF = microBatchDF.withColumn(
            "row_hash",
            sha2(
                to_json(
                    struct(*[col(c) for c in microBatchDF.columns if c not in ("ingest_time")])
                ),
                256
            )
        )
        microBatchDF = microBatchDF.withColumn("created_on", current_timestamp())
        microBatchDF = microBatchDF.withColumn("deleted_on", lit(None).cast("timestamp"))
        microBatchDF = microBatchDF.withColumn("current_flag", lit("Yes"))
        microBatchDF = microBatchDF.withColumn("valid_from", current_timestamp())
        microBatchDF = microBatchDF.withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))

        # Sanity check
        create_table_if_not_exists(spark, microBatchDF, dst_table_name)
        
        microBatchDF.createOrReplaceTempView("updates")
        spark.sql(
            f"""
            MERGE INTO {dst_table_name} t
            USING updates s
            ON {merge_condition} AND t.current_flag='Yes'
            WHEN MATCHED
                AND t.row_hash<>s.row_hash
            THEN UPDATE SET
                t.deleted_on=current_timestamp(),
                t.current_flag='No',
                t.valid_to=current_timestamp()
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
        .writeStream
        .queryName(dst_table_name)
        .options(**writeStreamOptions)
        .trigger(availableNow=True)
        .foreachBatch(upsert_to_gold)
        .outputMode("update")
        .start()
    )


