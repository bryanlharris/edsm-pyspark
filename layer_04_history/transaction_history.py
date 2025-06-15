import json
from pyspark.sql.functions import lit, expr
from functions.utility_functions import create_table_if_not_exists

def transaction_history(spark, full_table_name):
    transaction_table_name = f"{full_table_name}_transaction_history"

    # Create df
    df = (
        spark.sql(f"describe history {full_table_name}")
        .withColumn("table_name", lit(full_table_name))
        .withColumn('primary_key', expr(' coalesce(table_name, "") || "_" || coalesce(cast(version as string), "") '))
        .selectExpr("table_name", "* except (table_name)")
    )

    # Sanity check
    create_table_if_not_exists(spark, df, transaction_table_name)

    df.createOrReplaceTempView("df")
    spark.sql(f"""
        merge into {transaction_table_name} as target
        using df as source
        on target.primary_key = source.primary_key
        when matched then update set *
        when not matched then insert *
    """)