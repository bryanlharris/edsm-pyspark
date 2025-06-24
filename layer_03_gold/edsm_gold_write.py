

def edsm_gold_write(spark, settings, df):
    # Variables (json file)
    dst_table_name          = settings.get("dst_table_name")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    
    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
    change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])

    df.createOrReplaceTempView("updates")
    # Mark deletions only
    spark.sql(f"""
        MERGE INTO {dst_table_name} t
        USING updates s
        ON {merge_condition} AND t.current_flag='Yes'
        WHEN MATCHED AND ({change_condition}) THEN
            UPDATE SET
                t.deleted_on=s.ingest_time,
                t.current_flag='No',
                t.valid_to=s.ingest_time
    """)

    spark.sql(f"""
        INSERT INTO {dst_table_name}
        SELECT
            s.* EXCEPT (created_on, deleted_on, current_flag, valid_from, valid_to),
            s.ingest_time AS created_on,
            NULL AS deleted_on,
            'Yes' AS current_flag,
            s.ingest_time AS valid_from,
            CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS valid_to
        FROM updates s
        LEFT JOIN {dst_table_name} t
            ON {merge_condition} AND t.current_flag='Yes'
        WHERE t.current_flag IS NULL
    """)







