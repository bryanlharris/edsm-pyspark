
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    ArrayType,
    MapType,
)
from pyspark.sql.functions import (
    current_timestamp,
    when,
    col,
    to_timestamp,
    to_date,
    regexp_replace,
    sha2,
    lit,
    trim,
    struct,
    to_json,
    transform,
    array,
)
import re


def bronze_standard_transform(df, settings, spark):
    """Apply standard bronze layer transformations."""

    df = df.transform(clean_column_names)

    file_schema = settings.get("file_schema", [])
    rescue_in_schema = any(
        isinstance(f, dict) and f.get("name") == "_rescued_data" for f in file_schema
    )

    df = df.withColumn("ingest_time", current_timestamp())

    if not rescue_in_schema:
        df = df.transform(add_rescued_data)

    return df

def silver_standard_transform(df, settings, spark):
    """Apply common silver layer cleaning logic."""

    # Settings
    surrogate_key = settings.get("surrogate_key", [])
    column_map = settings.get("column_map", None)
    data_type_map = settings.get("data_type_map", None)
    use_row_hash = str(settings.get("use_row_hash", "false")).lower() == "true" and len(surrogate_key) > 0
    row_hash_col = settings.get("row_hash_col", "row_hash")

    return (
        df.transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(add_row_hash, surrogate_key, row_hash_col, use_row_hash)
    )


def silver_scd2_transform(df, settings, spark):
    """Prepare a DataFrame for SCD2 upserts in the silver layer."""

    return (
        df.transform(silver_standard_transform, settings, spark)
          .transform(add_scd2_columns, settings, spark)
    )


def add_scd2_columns(df, settings, spark):
    """Attach standard SCD2 tracking columns."""

    ingest_time_column = settings["ingest_time_column"]
    return (
        df.withColumn("created_on", col(ingest_time_column))
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("current_flag", lit("Yes"))
        .withColumn("valid_from", col(ingest_time_column))
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    )


def add_rescued_data(df):
    """Ensure a ``_rescued_data`` column exists in the DataFrame."""

    rescued_data_type = StringType()

    if "_rescued_data" in df.columns:
        return df
    else:
        return df.withColumn("_rescued_data", lit(None).cast(rescued_data_type))


def make_null_safe(col_expr, dtype):
    """Ensure nested fields exist so hashing comparisons are stable."""
    if isinstance(dtype, StructType):
        return struct(*[
            make_null_safe(col_expr[f.name], f.dataType).alias(f.name)
            for f in dtype.fields
        ])
    elif isinstance(dtype, ArrayType):
        return when(col_expr.isNull(), array().cast(dtype)) \
            .otherwise(transform(col_expr, lambda x: make_null_safe(x, dtype.elementType)))
    elif isinstance(dtype, MapType):
        return col_expr
    else:
        return when(col_expr.isNull(), lit(None).cast(dtype)).otherwise(col_expr)


def normalize_for_hash(df, fields):
    """Normalize fields to stable structures for hashing."""

    schema = df.schema
    fields_present = [f for f in fields if f in df.columns]
    return df.withColumn(
        "__normalized_struct__",
        struct(*[
            make_null_safe(col(f), schema[f].dataType).alias(f)
            for f in fields_present
        ])
    )

def add_row_hash(df, fields_to_hash, name="row_hash", use_row_hash=False):
    """Compute a hash over selected fields and store it in ``name``."""

    if not use_row_hash:
        return df

    df = normalize_for_hash(df, fields_to_hash)

    return df.withColumn(name, sha2(to_json(col("__normalized_struct__")), 256)).drop("__normalized_struct__")


def clean_column_names(df):
    """Normalize column names by removing spaces and invalid characters."""
    def clean(name):
        name = name.strip().lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^0-9a-zA-Z_]+", "", name)
        name = re.sub(r"_+", "_", name)
        return name

    def _rec(c, t):
        if isinstance(t, StructType):
            return struct(*[
                _rec(c[f.name], f.dataType).alias(clean(f.name))
                for f in t.fields
            ])
        if isinstance(t, ArrayType):
            return transform(c, lambda x: _rec(x, t.elementType))
        return c

    return df.select(*[
        _rec(col(f.name), f.dataType).alias(clean(f.name))
        for f in df.schema.fields
    ])


def trim_column_values(df, cols=None):
    """Strip whitespace from string columns."""

    if cols:
        target_cols = set(c for c in cols if c in df.columns)
    else:
        target_cols = set(c for c, t in df.dtypes if t == "string")

    replacements = {c: trim(col(c)).alias(c) for c in target_cols}
    new_cols = [replacements.get(c, col(c)) for c in df.columns]

    return df.select(new_cols)


def rename_columns(df, column_map=None):
    """Rename DataFrame columns using ``column_map``."""

    if not column_map:
        return df
    new_names = [column_map.get(c, c) for c in df.columns]
    return df.toDF(*new_names)


def cast_data_types(df, data_type_map=None):
    """Cast columns to the provided data types."""

    if not data_type_map:
        return df
    
    data_type_map   = {c: data_type_map[c] for c in df.columns if c in data_type_map}

    selected_columns = []
    for column_name in df.columns:
        if column_name in data_type_map:
            data_type = data_type_map[column_name]
            if data_type in ["integer", "double", "short", "float"]:
                selected_columns.append(col(column_name).cast(data_type).alias(column_name))
            elif data_type.startswith(("decimal", "numeric")):
                # Replace '$' and ',' so $4,000 -> 4000
                selected_columns.append(regexp_replace(col(column_name), '[$,]', '').cast(data_type).alias(column_name))
            elif data_type == "date":
                selected_columns.append(
                    when(col(column_name).rlike(r'\d{1,2}/\d{1,2}/\d{4}'), to_date(col(column_name), 'M/d/yyyy'))
                    .when(col(column_name).rlike(r'\d{1,2}-\d{1,2}-\d{4}'), to_date(col(column_name), 'd-M-yyyy'))
                    .when(col(column_name).rlike(r'\d{4}-\d{1,2}-\d{1,2}'), to_date(col(column_name), 'yyyy-M-d'))
                    .alias(column_name)
                )
            elif data_type == "timestamp":
                selected_columns.append(
                    when(col(column_name).rlike(r'\d{1,2}/\d{1,2}/\d{4}'), to_date(col(column_name), 'M/d/yyyy'))
                    .when(col(column_name).rlike(r'\d{1,2}-\d{1,2}-\d{4}'), to_date(col(column_name), 'd-M-yyyy'))
                    .otherwise(to_timestamp(col(column_name)))
                    .alias(column_name)
                )
            else:
                # If not one of the above data types, append it without changing
                # Execution would reach this point if the data_type from the column_map was not recognized
                selected_columns.append(col(column_name))
        else:
            # If column_name was not in data_type_map, just append it without changing
            # Execution would reach this point if the column was not in column_map
            selected_columns.append(col(column_name))

    return df.select(selected_columns)






