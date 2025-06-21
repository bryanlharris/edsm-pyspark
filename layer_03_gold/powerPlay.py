import json
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, struct, to_json, sha2, col, current_timestamp
from functions import create_table_if_not_exists

def powerPlay(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")

    (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
        .writeStream
        .queryName(dst_table_name)
        .options(**writeStreamOptions)
        .trigger(availableNow=True)
        .foreachBatch(powerPlay_upsert(spark, settings))
        .outputMode("update")
        .start()
    )




def powerPlay_upsert(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")

    def upsert(microBatchDF, batchId):
        microBatchDF = microBatchDF.withColumn("created_on", col("ingest_time"))
        microBatchDF = microBatchDF.withColumn("deleted_on", lit(None).cast("timestamp"))
        microBatchDF = microBatchDF.withColumn("current_flag", lit("Yes"))
        microBatchDF = microBatchDF.withColumn("valid_from", col("ingest_time"))
        microBatchDF = microBatchDF.withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    
        # fields_to_hash = composite_key + business_key
        # microBatchDF = microBatchDF.withColumn(
        #     "row_hash",
        #     sha2(to_json(struct(*[col(c) for c in fields_to_hash])),256)
        # )

        # Sanity check
        if batchId == 0:
            create_table_if_not_exists(spark, microBatchDF, dst_table_name)
        
        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
        if business_key:
            change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])
        else:
            change_condition = "FALSE"

        microBatchDF.createOrReplaceTempView("updates")
        spark.sql(f"""
            MERGE INTO {dst_table_name} t
            USING updates s
            ON {merge_condition} AND t.current_flag='Yes'
            WHEN MATCHED AND ({change_condition}) THEN
                UPDATE SET
                    t.deleted_on=s.ingest_time,
                    t.current_flag='No',
                    t.valid_to=s.ingest_time
        """)

        spark.sql(f"""
            INSERT INTO {dst_table_name}
            SELECT
                s.* EXCEPT (created_on, deleted_on, current_flag, valid_from, valid_to),
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
    
    return upsert






