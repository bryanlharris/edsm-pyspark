

def edsm_silver_read(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    readStreamOptions       = settings.get("readStreamOptions")

    return (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
    )
