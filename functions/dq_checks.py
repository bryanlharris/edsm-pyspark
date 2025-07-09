"""Custom data quality check functions for Databricks DQX.

This module defines helper functions used when profiling tables. They are
registered dynamically inside :func:`functions.quality.apply_dqx_checks` so
that importing :mod:`functions.quality` does not require PySpark.
"""
from __future__ import annotations

from functools import reduce


def min_max(df, column, min, max):
    """Return ``True`` if values fall within ``min`` and ``max``."""

    return df.filter((df[column] < min) | (df[column] > max)).count() == 0


def is_in(df, column, in_):
    """Return ``True`` if ``column`` values are contained in ``in_``."""

    return df.filter(~df[column].isin(in_)).count() == 0


def is_not_null_or_empty(df, column, trim_strings=False):
    """Return ``True`` if ``column`` is neither null nor empty."""

    from pyspark.sql import functions as F

    c = df[column]
    if trim_strings:
        c = F.trim(c)
    return df.filter(c.isNull() | (c == "")).count() == 0


def max_length(df, column, max):
    """Return ``True`` if ``column`` length is not greater than ``max``."""

    pattern = f".{{{max + 1},}}"
    return df.filter(df[column].rlike(pattern)).count() == 0


def matches_regex_list(df, column, patterns):
    """Return ``True`` if ``column`` matches any pattern in ``patterns``."""

    if not patterns:
        return True
    condition = reduce(lambda a, b: a | b, (df[column].rlike(p) for p in patterns))
    return df.filter(~condition).count() == 0


def is_nonzero(df, column):
    """Return ``True`` if ``column`` is not zero."""

    return df.filter(df[column] == 0).count() == 0


def starts_with_prefixes(df, column, prefixes):
    """Return ``True`` if ``column`` starts with any of ``prefixes``."""

    if not prefixes:
        return True
    condition = reduce(
        lambda a, b: a | b,
        (df[column].startswith(p) for p in prefixes),
    )
    return df.filter(~condition).count() == 0
