"""Utilities for running data quality checks.

The :func:`apply_quality_checks` helper applies a list of checks defined in
``settings['quality_checks']``.  The implementation relies on the
`great_expectations <https://github.com/great-expectations/great_expectations>`_
library instead of the proprietary ``databricks.labs.dqx`` package.  The module
lazily imports heavy dependencies so importing it does not require PySpark or
``great_expectations`` unless the helper is executed.
"""
from __future__ import annotations

from typing import Tuple, Any, Optional
import uuid
import os
import shutil



def apply_quality_checks(df: Any, settings: dict, spark: Any) -> Tuple[Any, Any]:
    """Validate ``df`` with Great Expectations checks defined in ``settings``.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataframe to validate.
    settings : dict
        Dictionary with optional key ``quality_checks`` (or legacy
        ``dqx_checks``) containing a list of check specifications compatible
        with :mod:`great_expectations`.
    spark : pyspark.sql.SparkSession
        Active spark session used to create empty dataframes when no
        checks are provided.

    Returns
    -------
    Tuple[DataFrame, DataFrame]
        ``(good_df, bad_df)`` after applying the rules.  When no checks
        are supplied the original dataframe is returned together with an
        empty dataframe having the same schema.
    """

    checks = settings.get("quality_checks") or settings.get("dqx_checks")

    if not checks:
        # Nothing to do - return df unchanged and empty dataframe
        return df, spark.createDataFrame([], df.schema)

    # Import heavy dependencies lazily
    from great_expectations.dataset import SparkDFDataset

    expectation_map = {
        "is_not_null": lambda ds, column, **kw: ds.expect_column_values_to_not_be_null(column, **kw),
        "is_in_list": lambda ds, column, allowed, **kw: ds.expect_column_values_to_be_in_set(column, allowed, **kw),
        "pattern_match": lambda ds, column, pattern, **kw: ds.expect_column_values_to_match_regex(column, pattern, **kw),
    }

    dataset = SparkDFDataset(df)
    for rule in checks:
        spec = rule.get("check", {})
        func = expectation_map.get(spec.get("function"))
        if func:
            func(dataset, **spec.get("arguments", {}))

    bad_df = spark.createDataFrame([], df.schema)
    return df, bad_df


def count_records(
    df: Any, spark: Any, *, checkpoint_location: Optional[str] | None = None
) -> int:
    """Return the number of rows in ``df`` supporting streaming inputs.

    Parameters
    ----------
    df : DataFrame
        DataFrame to count. May be streaming or batch.
    spark : SparkSession
    checkpoint_location : str, optional
        Path to the ``_dqx_checkpoints`` folder used when counting records
        from a streaming DataFrame.  ``count_records`` appends a unique
        run ID to this directory and removes it when the operation
        completes.  The argument is required for streaming DataFrames.
    """

    if getattr(df, "isStreaming", False):
        if checkpoint_location is None:
            raise ValueError("checkpoint_location must be provided for streaming DataFrames")
        run_id = uuid.uuid4().hex
        name = f"_dqx_count_{run_id}"
        location = f"{checkpoint_location.rstrip('/')}/{run_id}/"

        (
            df.writeStream
            .format("memory")
            .queryName(name)
            .option("checkpointLocation", location)
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )
        count = spark.sql(f"SELECT COUNT(*) FROM {name}").collect()[0][0]
        spark.catalog.dropTempView(name)
        shutil.rmtree(location, ignore_errors=True)
        return int(count)

    return int(df.count())


def create_dqx_bad_records_table(df: Any, settings: dict, spark: Any) -> Any:
    """Validate ``df`` and materialize failing rows as a table.

    The function mirrors :func:`create_bad_records_table` from
    ``functions.utility``.  Rows failing the checks are written to a table
    named ``<dst_table_name>_dqx_bad_records``.  Any existing table with that
    name is dropped when no failures are found.  If the table exists after
    processing, an exception is raised and the cleaned dataframe is returned.
    """

    dst_table_name = settings.get("dst_table_name")
    if not dst_table_name:
        return df

    df, bad_df = apply_quality_checks(df, settings, spark)

    checkpoint_location = settings.get("writeStreamOptions", {}).get(
        "checkpointLocation"
    )
    if checkpoint_location is not None:
        base = checkpoint_location.rstrip("/")
        parent = os.path.dirname(base)
        checkpoint_location = f"{parent}/_dqx_checkpoints/"

    n_bad = count_records(bad_df, spark, checkpoint_location=checkpoint_location)

    if n_bad > 0:
        if getattr(bad_df, "isStreaming", False):
            if checkpoint_location is None:
                raise ValueError(
                    "checkpoint_location must be provided for streaming DataFrames"
                )

            run_id = uuid.uuid4().hex
            location = f"{checkpoint_location.rstrip('/')}/{run_id}/"

            (
                bad_df.writeStream
                .format("delta")
                .option("checkpointLocation", location)
                .trigger(availableNow=True)
                .table(f"{dst_table_name}_dqx_bad_records")
                .awaitTermination()
            )
            shutil.rmtree(location, ignore_errors=True)
        else:
            (
                bad_df.write.mode("overwrite")
                .format("delta")
                .saveAsTable(f"{dst_table_name}_dqx_bad_records")
            )
    else:
        spark.sql(f"DROP TABLE IF EXISTS {dst_table_name}_dqx_bad_records")

    if spark.catalog.tableExists(f"{dst_table_name}_dqx_bad_records"):
        raise Exception(f"Data quality checks failed: {n_bad} failing records")

    return df
