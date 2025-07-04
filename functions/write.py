from functions.utility import create_table_if_not_exists, get_function
from functions.transform import silver_scd2_transform
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def overwrite_table(df, settings, spark):
    df.write.mode("overwrite").saveAsTable(settings["dst_table_name"])


def stream_write_table(df, settings, spark):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    writeStreamOptions      = settings.get("writeStreamOptions")

    # Write
    (
        df.writeStream
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("append")
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

def microbatch_upsert_fn(settings, spark):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    business_key           = settings.get("business_key")
    surrogate_key            = settings.get("surrogate_key")

    use_row_hash            = settings.get("use_row_hash", False)
    row_hash_col            = settings.get("row_hash_col", "row_hash")
    surrogate_key            = settings.get("surrogate_key")

    def upsert(microBatchDF, batchId):
        # Sanity check needs to be in its own place
        if batchId == 0:
            create_table_if_not_exists(spark, microBatchDF, dst_table_name)

        # from pyspark.sql.functions import row_number
        # from pyspark.sql.window import Window
        # if use_row_hash:
        #     window = Window.partitionBy(*business_key).orderBy(row_hash_col)
        # else:
        #     window = Window.partitionBy(*business_key).orderBy(*surrogate_key)
        # microBatchDF = microBatchDF.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")

        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in business_key])
        # change_condition = " or ".join([f"t.{k}<>s.{k}" for k in surrogate_key])
        if use_row_hash:
            change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
        else:
            change_condition = " or ".join([f"t.{k} <> s.{k}" for k in surrogate_key])

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


def _scd2_upsert(df, settings, spark):
    """Common logic for performing SCD2 merge operations."""
    dst_table_name = settings.get("dst_table_name")
    business_key = settings.get("business_key")
    surrogate_key = settings.get("surrogate_key")
    ingest_time_column = settings.get("ingest_time_column")
    use_row_hash = settings.get("use_row_hash", False)
    row_hash_col = settings.get("row_hash_col", "row_hash")

    window = Window.partitionBy(*business_key).orderBy(col(ingest_time_column).desc())
    df = (
        df.withColumn("rn", row_number().over(window))
        .filter("rn = 1")
        .drop("rn")
    )

    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in business_key])
    if use_row_hash:
        change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
    else:
        change_condition = " or ".join([f"t.{k} <> s.{k}" for k in surrogate_key])

    df.createOrReplaceTempView("updates")

    spark.sql(
        f"""
        MERGE INTO {dst_table_name} t
        USING updates s
        ON {merge_condition} AND t.current_flag='Yes'
        WHEN MATCHED AND ({change_condition}) THEN
            UPDATE SET
                t.deleted_on=s.{ingest_time_column},
                t.current_flag='No',
                t.valid_to=s.{ingest_time_column}
        """
    )

    spark.sql(
        f"""
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
        """
    )


def microbatch_upsert_scd2_fn(settings, spark):
    def upsert(microBatchDF, batchId):
        print(f"Starting batchId: {batchId}, count: {microBatchDF.count()}")
        microBatchDF.show(5, truncate=False)

        _scd2_upsert(microBatchDF, settings, spark)

    return upsert

def batch_upsert_scd2(df, settings, spark):
    _scd2_upsert(df, settings, spark)






def write_upsert_snapshot(df, settings, spark):
    dst_table = settings["dst_table_name"]
    business_key = settings["business_key"]
    ingest_time_col = settings["ingest_time_column"]

    window = Window.partitionBy(*business_key).orderBy(col(ingest_time_col).desc())
    df = df.withColumn("row_num", row_number().over(window)).filter("row_num = 1").drop("row_num")
    df.createOrReplaceTempView("updates")

    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in business_key])

    spark.sql(f"""
    MERGE INTO {dst_table} AS target
    USING updates AS source
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)



