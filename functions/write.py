from functions import create_table_if_not_exists, get_function, silver_scd2_transform
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def overwrite_table(df, settings, spark):
    df.write.mode("overwrite").saveAsTable(settings["dst_table_name"])


def stream_write_table(df, settings, spark):
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


def stream_upsert_table(df, settings, spark):
    upsert_func = get_function(settings.get("upsert_function"))
    return (
        df.writeStream
        .queryName(settings.get("dst_table_name"))
        .options(**settings.get("writeStreamOptions"))
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(settings, spark))
        .outputMode("update")
        .start()
    )

def upsert_microbatch(settings, spark):
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
        # if use_row_hash:
        #     window = Window.partitionBy(*composite_key).orderBy(row_hash_col)
        # else:
        #     window = Window.partitionBy(*composite_key).orderBy(*business_key)
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


## Let's you have a silver SCD2 table based on streaming
## (Bronze can have duplicates on the composite_key, it uses the latest)
def stream_write_scd2_table(df, settings, spark):
    composite_key = settings["composite_key"]
    ingest_time_column = settings["ingest_time_column"]

    upsert_func = get_function(settings.get("upsert_function"))
    return (
        df.writeStream
        .queryName(settings["dst_table_name"])
        .options(**settings["writeStreamOptions"])
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(settings, spark))
        .outputMode("update")
        .start()
    )

def scd2_upsert(settings, spark):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    ingest_time_column      = settings.get("ingest_time_column")
    use_row_hash            = settings.get("use_row_hash", False)
    row_hash_col            = settings.get("row_hash_col", "row_hash")
    
    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
    # change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])
    if use_row_hash:
        change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
    else:
        change_condition = " or ".join([f"t.{k} <> s.{k}" for k in business_key])

    def upsert(microBatchDF, batchId):
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, col

        window = Window.partitionBy(*composite_key).orderBy(col(ingest_time_column).desc())
        microBatchDF = (
            microBatchDF.withColumn("rn", row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
        )

        microBatchDF.createOrReplaceTempView("updates")
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

    return upsert

## Let's you catch up a silver SCD2 table based on streaming
## (Bronze assumed to have duplicates on the composite_key, it loops through them)
## This is all untested by the way.
# To use this, set your settings like this
#     "read_function": "functions.stream_read_table",                   # normal readstream
#     "transform_function": "functions.silver_scd2_catchup_transform",  # dummy transforms (real transforms in upsert)
#     "write_function": "functions.stream_scd2_catchup_write",          # Streams entire batch (trigger=availableNow)
#     "upsert_function": "functions.scd2_catchup_outer_upsert",         # outer upsert, loops through times, runs inner upsert one by one
def stream_scd2_catchup_write(df, settings, spark):
    composite_key = settings["composite_key"]
    ingest_time_column = settings["ingest_time_column"]

    upsert_func = get_function(settings.get("upsert_function"))
    return (
        df.writeStream
        .queryName(settings["dst_table_name"])
        .options(**settings["writeStreamOptions"])
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(settings, spark))
        .outputMode("update")
        .start()
    )

def scd2_catchup_outer_upsert(settings, spark):
    def upsert_fn(df, batchId):
        upsert = scd2_upsert(settings, spark)

        times = (
            df.select("derived_ingest_time")
              .distinct()
              .orderBy("derived_ingest_time")
              .collect()
        )

        for row in times:
            ts = row["derived_ingest_time"]
            batch = df.filter(col("derived_ingest_time") == ts)
            batch = silver_scd2_transform(batch, settings, spark)
            upsert(batch, batchId)

    return upsert_fn



## Let's you have a gold SCD2 table based on static deduplicated silver
## (Silver cannot have duplicates on the composite_key)
def scd2_write(df, settings, spark):
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





