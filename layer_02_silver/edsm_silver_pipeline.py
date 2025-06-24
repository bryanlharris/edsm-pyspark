

def edsm_silver_pipeline(spark, settings):
    df = edsm_silver_read(spark, settings)
    df = edsm_silver_transform(spark, settings, df)
    query = edsm_silver_write(spark, settings, df)


