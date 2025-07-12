"""Custom data quality check functions for Databricks DQX.

This module defines helper functions used when profiling tables. They are
registered dynamically inside :func:`functions.quality.apply_quality_checks` so
that importing :mod:`functions.quality` does not require PySpark.
"""
from __future__ import annotations

from functools import reduce


def min_max(column, min, max):
    """Return a column validating that values fall within ``min`` and ``max``."""

    from pyspark.sql import functions as F

    col_expr = F.col(column) if isinstance(column, str) else column
    condition = (col_expr < F.lit(min)) | (col_expr > F.lit(max))
    alias = f"{str(column).replace('.', '_')}_min_max"
    return F.when(condition, F.lit(f"Value out of range [{min}, {max}]"))\
        .otherwise(F.lit(None)).alias(alias)


def is_in(column, in_):
    """Return a column validating that values are contained in ``in_``."""

    from pyspark.sql import functions as F

    col_expr = F.col(column) if isinstance(column, str) else column
    condition = ~col_expr.isin(*in_)
    alias = f"{str(column).replace('.', '_')}_is_in"
    return (
        F.when(condition, F.lit(f"Value not in list {in_}"))
        .otherwise(F.lit(None))
        .alias(alias)
    )


def is_not_null_or_empty(column, trim_strings=False):
    """Return a column validating ``column`` is neither null nor empty."""

    from pyspark.sql import functions as F

    col_expr = F.col(column) if isinstance(column, str) else column
    if trim_strings:
        col_expr = F.trim(col_expr)
    condition = col_expr.isNull() | (col_expr == "")
    alias = f"{str(column).replace('.', '_')}_is_not_null_or_empty"
    return (
        F.when(condition, F.lit("Value is null or empty"))
        .otherwise(F.lit(None))
        .alias(alias)
    )


def max_length(column, max):
    """Return a column validating ``column`` length is not greater than ``max``."""

    from pyspark.sql import functions as F

    col_expr = F.col(column) if isinstance(column, str) else column
    pattern = f".{{{max + 1},}}"
    condition = col_expr.rlike(pattern)
    alias = f"{str(column).replace('.', '_')}_max_length"
    return (
        F.when(condition, F.lit(f"Length greater than {max}"))
        .otherwise(F.lit(None))
        .alias(alias)
    )


def matches_regex_list(column, patterns):
    """Return a column validating ``column`` matches any pattern in ``patterns``."""

    from pyspark.sql import functions as F

    if not patterns:
        return F.lit(None).alias(f"{str(column).replace('.', '_')}_regex")
    col_expr = F.col(column) if isinstance(column, str) else column
    condition = reduce(lambda a, b: a | b, (col_expr.rlike(p) for p in patterns))
    alias = f"{str(column).replace('.', '_')}_regex"
    return (
        F.when(~condition, F.lit("Value does not match regex list"))
        .otherwise(F.lit(None))
        .alias(alias)
    )


def pattern_match(column, pattern):
    """Return a column validating ``column`` matches ``pattern``."""

    return matches_regex_list(column, [pattern])


def is_nonzero(column):
    """Return a column validating ``column`` is not zero."""

    from pyspark.sql import functions as F

    col_expr = F.col(column) if isinstance(column, str) else column
    condition = col_expr == F.lit(0)
    alias = f"{str(column).replace('.', '_')}_is_nonzero"
    return (
        F.when(condition, F.lit("Value is zero"))
        .otherwise(F.lit(None))
        .alias(alias)
    )


def starts_with_prefixes(column, prefixes):
    """Return a column validating ``column`` starts with any of ``prefixes``."""

    from pyspark.sql import functions as F

    if not prefixes:
        return F.lit(None).alias(f"{str(column).replace('.', '_')}_prefix")
    col_expr = F.col(column) if isinstance(column, str) else column
    condition = reduce(
        lambda a, b: a | b,
        (col_expr.startswith(p) for p in prefixes),
    )
    alias = f"{str(column).replace('.', '_')}_prefix"
    return (
        F.when(~condition, F.lit("Value does not start with prefixes"))
        .otherwise(F.lit(None))
        .alias(alias)
    )
