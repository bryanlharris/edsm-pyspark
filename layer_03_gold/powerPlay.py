import json
from delta.tables import DeltaTable
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
        microBatchDF = microBatchDF.withColumn("created_on", col("ingest_time"))
        microBatchDF = microBatchDF.withColumn("deleted_on", lit(None).cast("timestamp"))
        microBatchDF = microBatchDF.withColumn("current_flag", lit("Yes"))
        microBatchDF = microBatchDF.withColumn("valid_from", col("ingest_time"))
        microBatchDF = microBatchDF.withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    
        ignore_cols = (
            "date", "ingest_time", "file_path", "file_modification_time", "source_metadata",
            "created_on", "deleted_on", "current_flag", "valid_from", "valid_to"
        )
        microBatchDF = microBatchDF.withColumn(
            "row_hash",
            sha2(to_json(struct(*[col(c) for c in microBatchDF.columns if c not in ignore_cols])),256)
        )

        # Sanity check
        create_table_if_not_exists(spark, microBatchDF, dst_table_name)
        
        microBatchDF.createOrReplaceTempView("updates")
        spark.sql(f"""
            MERGE INTO {dst_table_name} t
            USING updates s
            ON {merge_condition} AND t.current_flag='Yes'
            WHEN MATCHED AND t.row_hash<>s.row_hash THEN
                UPDATE SET
                    t.deleted_on=s.ingest_time,
                    t.current_flag='No',
                    t.valid_to=s.ingest_time
        """)

        spark.sql(f"""
            INSERT INTO {dst_table_name}
            SELECT
                s.* EXCEPT (current_flag, deleted_on, valid_from, created_on, valid_to),
                s.ingest_time AS created_on,
                NULL AS deleted_on,
                'Yes' AS current_flag,
                s.ingest_time AS valid_from,
                CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS valid_to
            FROM updates s
            LEFT JOIN {dst_table_name} t
                ON {merge_condition} AND t.current_flag='Yes'
            WHERE t.current_flag IS NULL
        """)

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









