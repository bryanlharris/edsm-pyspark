from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from functions import create_table_if_not_exists

def edsm_silver_write(spark, settings, df):
    return (
        df.writeStream
        .queryName(settings.get("dst_table_name"))
        .options(**settings.get("writeStreamOptions"))
        .trigger(availableNow=True)
        .foreachBatch(edsm_silver_upsert(spark, settings))
        .outputMode("update")
        .start()
    )


def edsm_silver_upsert(spark, settings):
    # Variables
    dst_table_name          = settings.get("dst_table_name")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")

    def upsert(microBatchDF, batchId):
        # Sanity check needs to be in its own place
        if batchId == 0:
            create_table_if_not_exists(spark, microBatchDF, dst_table_name)

        # window = Window.partitionBy(*composite_key).orderBy(*business_key)
        # microBatchDF = microBatchDF.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")

        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in composite_key])
        change_condition = " or ".join([f"t.{k}<>s.{k}" for k in business_key])

        microBatchDF.createOrReplaceTempView("updates")
        spark.sql(
            f"""
            MERGE INTO {dst_table_name} t
            USING updates s
            ON {merge_condition}
            WHEN MATCHED AND ({change_condition}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    return upsert






