"""Utilities for integrating Databricks DQX checks.

This module exposes a helper function :func:`apply_dqx_checks` which
runs data quality checks using the `databricks-labs-dqx` library.  The
function is written so that importing this module does not require
``pyspark`` or ``databricks-labs-dqx``.  Dependencies are loaded only
when the helper is executed.
"""
from __future__ import annotations

from typing import Tuple, Any


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
