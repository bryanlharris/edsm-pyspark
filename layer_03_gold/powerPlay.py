import json
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, isnull, lit
from functions import create_table_if_not_exists

def powerPlay(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    # pk                      = settings.get("pk")
    pk2                     = settings.get("pk2")
    merge_columns           = settings.get("merge_columns")

    # df = spark.read.table(src_table_name)

    ##################################################
    ### Some of these are for the temporal columns ###
    ##################################################
    # Need: current_flag                             #
    # Need: created_on                               #
    # Need: deleted_on                               #
    ##################################################
    df = (
        spark.readStream
        .format("delta")
        .options(**readStreamOptions)
        .table(src_table_name)
        .withColumn('current_flag', lit('Y'))
        .withColumn("created_on", current_timestamp())
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("effective_dt", current_timestamp())
    )

    ############################################
    ### Your normal table transforms go here ###
    ############################################

    ################################################################
    ### This creates a column called primary_key which is a hash ###
    ################################################################
    df.createOrReplaceTempView("df")
    supported=["string","boolean","tinyint","smallint","int","bigint",
               "float","double","decimal","date","timestamp"]
    columns = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString().split("(")[0] in supported
    ]
    columns.remove("current_flag")
    columns.remove("created_on")
    columns.remove("effective_dt")
    coalesced = [f"COALESCE(cast({col} as string), '')" for col in columns]
    sqlString = "SELECT *,\n" + "  SHA2(CONCAT(" + ", '-', ".join(coalesced) + "), 256) as primary_key\n" + f" FROM df"
    df = spark.sql(sqlString)

    #######################################################
    ### Rearrange the columns so the temporal are first ###
    #######################################################
    columns = df.columns
    columns.remove("current_flag")
    columns.remove("created_on")
    columns.remove("deleted_on")
    columns = ["current_flag", "created_on", "deleted_on"] + columns
    df = df.select(*columns)

    # Sanity check
    create_table_if_not_exists(spark, df, dst_table_name)

    #######################################
    ### These are to prevent duplicates ###
    #######################################
    # Need to ignore certain columns
    #
    # Ignore: current_flag
    # Ignore: created_on
    # Ignore: effective_dt
    #######################
    def upsert_batch(batch_df, batch_id):
        delta = DeltaTable.forName(spark, dst_table_name)
        existing = delta.toDF().filter("current_flag = 'Y'")
        new = (
            batch_df
            .drop("current_flag", "created_on", "effective_dt")
            .exceptAll(existing.drop("current_flag", "created_on", "effective_dt"))
            .dropDuplicates(merge_columns)
            .withColumn("current_flag", lit("Y"))
            .withColumn("created_on", current_timestamp())
            .withColumn("deleted_on", lit(None).cast("timestamp"))
            .withColumn("effective_dt", current_timestamp())
        )
        delta.alias("t") \
            .merge(new.alias("s"),
                " AND ".join(f"t.{c}=s.{c}" for c in merge_columns)) \
            .whenNotMatchedInsertAll() \
            .execute()

        # Delete missing records
        src = spark.table(src_table_name)
        dst = delta.toDF()
        missing = (
            dst
            .join(src, merge_columns, "leftanti")
            .filter("current_flag = 'Y' AND deleted_on IS NULL")
            .select(pk2)
        )
        delta.alias("d") \
            .merge(missing.alias("s"), f"s.{pk2}=d.{pk2}") \
            .whenMatchedUpdate(set={'current_flag': lit('N'),
                                'deleted_on': current_timestamp(),
                                'effective_dt': current_timestamp()}) \
            .execute()

        # Mark non-current records
        key_expr = ",".join(merge_columns)
        on_expr  = " AND ".join(f"t.{k}=s.{k}" for k in merge_columns)
        spark.sql(f"""
            MERGE INTO {dst_table_name} t
            USING (
            SELECT {key_expr}, MAX(created_on) AS max_created_on
            FROM {dst_table_name}
            GROUP BY {key_expr}
            ) s
            ON {on_expr}
            WHEN MATCHED AND t.created_on <> s.max_created_on
            THEN UPDATE SET t.current_flag='N'
        """)


    query = (
        df.writeStream
        .queryName(dst_table_name)
        .options(**writeStreamOptions)
        .trigger(availableNow=True)
        .foreachBatch(upsert_batch)
        .outputMode("update")
        .start()
    )
    




































