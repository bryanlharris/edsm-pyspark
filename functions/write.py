def stream_write_table(spark, settings, df):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    writeStreamOptions      = settings.get("writeStreamOptions")
    writeStream_format      = settings.get("writeStream_format")
    writeStream_outputMode  = settings.get("writeStream_outputMode")

    # Write
    (
        df.writeStream
        .format(writeStream_format)
        .options(**writeStreamOptions)
        .outputMode(writeStream_outputMode)
        .trigger(availableNow=True)
        .table(dst_table_name)
    )


from functions import create_table_if_not_exists, get_function

def stream_upsert_table(spark, settings, df):
    upsert_func = get_function(settings.get("upsert_function"))
    return (
        df.writeStream
        .queryName(settings.get("dst_table_name"))
        .options(**settings.get("writeStreamOptions"))
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(spark, settings))
        .outputMode("update")
        .start()
    )

def upsert_microbatch(spark, settings):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")

    use_row_hash            = settings.get("use_row_hash", False)
    row_hash_col            = settings.get("row_hash_col", "row_hash")
    business_key            = settings.get("business_key")

    def upsert(microBatchDF, batchId):
        # Sanity check needs to be in its own place
        if batchId == 0:
            create_table_if_not_exists(spark, microBatchDF, dst_table_name)

        # from pyspark.sql.functions import row_number
        # from pyspark.sql.window import Window
        # window = Window.partitionBy(*composite_key).orderBy(*business_key)
        # microBatchDF = microBatchDF.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")

        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
        # change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])
        if use_row_hash:
            change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
        else:
            change_condition = " or ".join([f"t.{k} <> s.{k}" for k in business_key])

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


def scd2_write(spark, settings, df):
    # Variables (json file)
    dst_table_name          = settings.get("dst_table_name")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    ingest_time_column      = settings.get("ingest_time_column")
    
    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
    change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])

    df.createOrReplaceTempView("updates")
    # Mark deletions only
    spark.sql(f"""
        MERGE INTO {dst_table_name} t
        USING updates s
        ON {merge_condition} AND t.current_flag='Yes'
        WHEN MATCHED AND ({change_condition}) THEN
            UPDATE SET
                t.deleted_on=s.{ingest_time_column},
                t.current_flag='No',
                t.valid_to=s.{ingest_time_column}
    """)

    spark.sql(f"""
        INSERT INTO {dst_table_name}
        SELECT
            s.* EXCEPT (created_on, deleted_on, current_flag, valid_from, valid_to),
            s.{ingest_time_column} AS created_on,
            NULL AS deleted_on,
            'Yes' AS current_flag,
            s.{ingest_time_column} AS valid_from,
            CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS valid_to
        FROM updates s
        LEFT JOIN {dst_table_name} t
            ON {merge_condition} AND t.current_flag='Yes'
        WHERE t.current_flag IS NULL
    """)





