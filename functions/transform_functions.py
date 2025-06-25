
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import when, col, to_timestamp, to_date, regexp_replace
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, trim, struct
from pyspark.sql.functions import sha2, to_json, struct
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, struct, transform
import re


def add_rescued_data(df):
    # rescued_data_type = StructType([
    #     StructField("columnName", StringType(), True),
    #     StructField("columnValue", StringType(), True)
    # ])

    rescued_data_type = StringType()

    if '_rescued_data' in df.columns:
        return df
    else:
        return df.withColumn("_rescued_data", lit(None).cast(rescued_data_type))



def add_source_metadata(df):
    metadata_type = StructType([
        StructField("file_path", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("file_block_start", LongType(), True),
        StructField("file_block_length", LongType(), True),
        StructField("file_modification_time", TimestampType(), True)
    ])
    if '_metadata' in df.columns:
        return df.withColumn("source_metadata", col("_metadata"))
    else:
        return df.withColumn("source_metadata", lit(None).cast(metadata_type))


# def rename_space_columns(df):
#     def _rec(c, t):
#         if isinstance(t, StructType):
#             return struct(*[_rec(c[f.name], f.dataType).alias(f.name.replace(" ", "_")) for f in t.fields])
#         if isinstance(t, ArrayType):
#             return transform(c, lambda x: _rec(x, t.elementType))
#         return c

#     special_columns = ["_metadata", "_rescued_data"]

#     fields = [f for f in df.schema.fields if f.name not in special_columns]

#     return df.select(
#         *[_rec(col(f.name), f.dataType).alias(f.name.replace(" ", "_")) for f in fields],
#         *[col(c) for c in df.columns if c in special_columns]
#     )


def rename_space_columns(df):
    def _rec(c, t):
        if isinstance(t, StructType):
            return struct(*[_rec(c[f.name], f.dataType).alias(f.name.replace(" ", "_")) for f in t.fields])
        if isinstance(t, ArrayType):
            return transform(c, lambda x: _rec(x, t.elementType))
        return c

    return df.select(*[_rec(col(f.name), f.dataType).alias(f.name.replace(" ", "_")) for f in df.schema.fields])


def row_hash(df, fields_to_hash, name="row_hash"):
    # fields_to_hash = composite_key + business_key
    df = df.withColumn(
        name,
        sha2(to_json(struct(*[col(c) for c in fields_to_hash])),256)
    )
    return df


def clean_column_names(df):
    column_map = {}
    for col_name in df.columns:
        new_name = col_name.strip().lower()
        new_name = re.sub(r"\s+", "_", new_name)
        new_name = re.sub(r"[^0-9a-zA-Z_]+", "", new_name)
        new_name = re.sub(r"_+", "_", new_name)
        column_map[col_name] = new_name
    return df.transform(rename_columns, column_map)




def trim_columns(df, cols=None):
    if cols:
        target_cols = [c for c in cols if c in df.columns]
    else:
        target_cols = [c for c, t in df.dtypes if t == 'string']
    for c in target_cols:
        df = df.withColumn(c, trim(col(c)))
    return df


def rename_columns(df, column_map=None):
    if not column_map:
        return df
    new_names = [column_map.get(c, c) for c in df.columns]
    return df.select(renamed)


def cast_data_types(df, data_type_map=None):
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






