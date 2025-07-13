from functions.utility import get_function
from functions.transform import silver_scd2_transform
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pathlib import Path


def _register_table(path, spark, table_name=None):
    """Register ``path`` as a Delta table and return the table name."""

    name = table_name or Path(path).name
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} USING DELTA LOCATION '{path}'")
    return name


def overwrite_table(df, settings, spark):
    """Overwrite the destination table with ``df``.

    ``settings`` must provide ``dst_table_path`` which is the location of the
    Delta table on disk.
    """

    dst_path = settings.get("dst_table_path")
    if not dst_path:
        raise KeyError("dst_table_path must be provided")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .save(dst_path)
    )


def stream_write_table(df, settings, spark):
    """Write a streaming DataFrame directly to a Delta table."""

    dst_table_path = settings.get("dst_table_path")
    if not dst_table_path:
        raise KeyError("dst_table_path must be provided")

    writeStreamOptions = settings.get("writeStreamOptions")

    return (
        df.writeStream
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(dst_table_path)
    )


def stream_upsert_table(df, settings, spark):
    """Apply an upsert function to each streaming micro-batch."""

    upsert_func = get_function(settings.get("upsert_function"))
    dst_table_path = settings.get("dst_table_path")
    if not dst_table_path:
        raise KeyError("dst_table_path must be provided")

    return (
        df.writeStream
        .queryName(dst_table_path)
        .options(**settings.get("writeStreamOptions"))
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(settings, spark))
        .outputMode("update")
        .start(dst_table_path)
    )


def _simple_merge(df, settings, spark):
    """Perform the standard merge logic used for non-SCD2 upserts."""
    dst_table_path = settings.get("dst_table_path")
    dst_table_name = _register_table(dst_table_path, spark)
    business_key = settings.get("business_key")
    surrogate_key = settings.get("surrogate_key")

    use_row_hash = str(settings.get("use_row_hash", "false")).lower() == "true"
    row_hash_col = settings.get("row_hash_col", "row_hash")

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
        ON {merge_condition}
        WHEN MATCHED AND ({change_condition}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def upsert_table(df, settings, spark, *, scd2=False, foreach_batch=False, batch_id=None):
    """Generic helper to upsert ``df`` into the destination table.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame or micro-batch DataFrame when used with ``foreachBatch``.
    settings : dict
        Configuration dictionary containing table names and keys.
    spark : SparkSession
    scd2 : bool, optional
        When ``True`` apply SCD2 merge logic. Defaults to ``False``.
    foreach_batch : bool, optional
        Set to ``True`` when called inside a ``foreachBatch`` loop so the
        destination table can be created on the first micro-batch. Defaults to
        ``False``.
    batch_id : int, optional
        Current micro-batch id. Only used when ``foreach_batch`` is ``True``.
    """

    if foreach_batch and batch_id == 0:
        dst_path = settings.get("dst_table_path")
        (
            spark.createDataFrame([], df.schema)
            .write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .save(dst_path)
        )
        _register_table(dst_path, spark)

    if scd2:
        _scd2_upsert(df, settings, spark)
    else:
        _simple_merge(df, settings, spark)

def microbatch_upsert_fn(settings, spark):
    """Wrapper used in ``foreachBatch`` for standard upserts."""

    def upsert(microBatchDF, batchId):
        upsert_table(
            microBatchDF,
            settings,
            spark,
            scd2=False,
            foreach_batch=True,
            batch_id=batchId,
        )

    return upsert


def _scd2_upsert(df, settings, spark):
    """Common logic for performing SCD2 merge operations."""
    dst_table_path = settings.get("dst_table_path")
    dst_table_name = _register_table(dst_table_path, spark)
    business_key = settings.get("business_key")
    surrogate_key = settings.get("surrogate_key")
    ingest_time_column = settings.get("ingest_time_column")
    use_row_hash = str(settings.get("use_row_hash", "false")).lower() == "true"
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
    """Wrapper used in ``foreachBatch`` for SCD2 style upserts."""

    def upsert(microBatchDF, batchId):
        print(f"Starting batchId: {batchId}, count: {microBatchDF.count()}")
        microBatchDF.show(5, truncate=False)

        upsert_table(
            microBatchDF,
            settings,
            spark,
            scd2=True,
            foreach_batch=True,
            batch_id=batchId,
        )

    return upsert

def batch_upsert_scd2(df, settings, spark):
    """Thin wrapper for SCD2 batch upserts."""

    upsert_table(df, settings, spark, scd2=True, foreach_batch=False)






def write_upsert_snapshot(df, settings, spark):
    """Write the latest records per business key into a snapshot table."""
    dst_table_path = settings["dst_table_path"]
    dst_table = _register_table(dst_table_path, spark)
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



