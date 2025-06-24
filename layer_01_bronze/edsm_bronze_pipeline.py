

def edsm_bronze_pipeline(spark, settings):
    df = edsm_bronze_read(spark, settings)
    df = edsm_bronze_transform(spark, settings, df)
    query = edsm_bronze_write(spark, settings, df)











