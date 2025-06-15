import json
from pyspark.sql.functions import current_timestamp, isnull, lit
from functions.utility_functions import create_table_if_not_exists

def temporal_write(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    pk                      = settings.get("pk")
    pk2                     = settings.get("pk2")
    merge_columns           = settings.get("merge_columns")

    # Read
    # df = (
    #     spark.readStream
    #     .format("delta")
    #     .options(**readStreamOptions)
    #     .table(src_table_name)
    # )

    df = spark.read.table(src_table_name)

    ############################################
    ### Your normal table transforms go here ###
    ############################################

    ##################################################
    ### Some of these are for the temporal columns ###
    ##################################################
    # Need: current_flag                             #
    # Need: created_on                               #
    # Need: deleted_on                               #
    ##################################################
    df = (
        df.toDF(*[c.lower() for c in df.columns])
        .withColumn('current_flag', lit('Y'))
        .withColumn("created_on", current_timestamp())
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("effective_dt", current_timestamp())
    )

    ################################################################
    ### This creates a column called primary_key which is a hash ###
    ################################################################
    df.createOrReplaceTempView("df")
    columns = [
        f.name for f in df.schema.fields 
        if f.dataType.simpleString() in [ "string", "int", "decimal", "date", "timestamp" ]
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
    df.createOrReplaceTempView("a")
    df1 = spark.table(dst_table_name)
    df1.createOrReplaceTempView("b")

    new = spark.sql(f"""
        select *
            except (current_flag, created_on, effective_dt)
        from a
        except
        select *
            except (current_flag, created_on, effective_dt)
        from b
        where current_flag = "Y";
    """)
    new = new.select(*merge_columns)
    df = df.join(new, merge_columns, "inner")

    #######################################
    ### Drop complete record duplicates ###
    #######################################
    df = df.dropDuplicates()

    ###########################
    ### Append to the table ###
    ###########################
    df.write.mode("append").format("delta").saveAsTable(dst_table_name)

    ##############################################################################
    ### This finds primary keys missing from the source side (deleted records) ###
    ##############################################################################
    # pk: the numerical primary key from the source
    # pk2: the hash of all columns primary key
    ##############################################################################
    df1 = spark.sql(f"select * from {src_table_name}")
    df2 = spark.sql(f"select * from {dst_table_name}")
    df2 = df2.join(df1, merge_columns, "leftanti")
    df2 = df2.filter((isnull(df2.deleted_on)) & (df2.current_flag == 'Y'))
    missing = df2.select(pk2)

    #####################################################
    ### For every missing key, change certain columns ###
    #####################################################
    # Change: current_flag
    # Change: deleted_on
    # Change: effective_dt
    #####################################################
    deltaTable = DeltaTable.forName(spark, dst_table_name)
    delete = ( deltaTable.alias("d")
                        .merge(missing.alias("s"), f"s.{pk2} = d.{pk2}")
                        .whenMatchedUpdate(set = {
                            "current_flag": lit("N"),
                            "deleted_on": current_timestamp(),
                            "effective_dt": current_timestamp()
                        })
                        .execute()
            )
    
    ##############################################################################
    ### This sets every active to "N" except the newest record per primary key ###
    ##############################################################################
    key_expr = ",".join(merge_columns)
    on_expr = " AND ".join([f"t.{k} = s.{k}" for k in merge_columns])

    spark.sql(f"""
        MERGE INTO {dst_table_name} t
        USING (
            SELECT
                {key_expr},
                MAX(created_on) AS max_created_on
            FROM {dst_table_name}
            GROUP BY {key_expr}
        ) s
        ON {on_expr}
        WHEN MATCHED AND t.created_on <> s.max_created_on THEN
            UPDATE SET t.current_flag = 'N'
    """)








































