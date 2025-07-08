"""Utilities for integrating Databricks DQX checks.

This module exposes a helper function :func:`apply_dqx_checks` which
runs data quality checks using the `databricks-labs-dqx` library.  The
function is written so that importing this module does not require
``pyspark`` or ``databricks-labs-dqx``.  Dependencies are loaded only
when the helper is executed.
"""
from __future__ import annotations

from typing import Tuple, Any, Optional
import uuid
import os
import shutil


def apply_dqx_checks(df: Any, settings: dict, spark: Any) -> Tuple[Any, Any]:
    """Apply DQX quality rules to ``df``.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataframe to validate.
    settings : dict
        Dictionary with optional key ``dqx_checks`` containing a list of
        check dictionaries in DQX format.
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

    checks = settings.get("dqx_checks")

    if not checks:
        # Nothing to do - return df unchanged and empty dataframe
        return df, spark.createDataFrame([], df.schema)

    # Import heavy dependencies lazily
    from databricks.labs.dqx.engine import DQEngineCore

    class _DummyCurrentUser:
        def me(self):
            return {}

    class _DummyConfig:
        _product_info = ("dqx", "0.0")

    class _DummyWS:
        def __init__(self):
            self.current_user = _DummyCurrentUser()
            self.config = _DummyConfig()

    dq_engine = DQEngineCore(_DummyWS(), spark)
    good_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)
    return good_df, bad_df


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
