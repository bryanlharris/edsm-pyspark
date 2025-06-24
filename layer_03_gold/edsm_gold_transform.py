
from pyspark.sql.functions import lit, col

def edsm_gold_transform(spark, settings, df):
    return (
        df.withColumn("created_on", col("ingest_time"))
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("current_flag", lit("Yes"))
        .withColumn("valid_from", col("ingest_time"))
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    )






