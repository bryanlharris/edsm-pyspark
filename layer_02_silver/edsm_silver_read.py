

def edsm_silver_read(spark, settings):
    # Variables (json file)
    src_table_name          = settings.get("src_table_name")
    dst_table_name          = settings.get("dst_table_name")
    readStreamOptions       = settings.get("readStreamOptions")
    writeStreamOptions      = settings.get("writeStreamOptions")
    composite_key           = settings.get("composite_key")
    business_key            = settings.get("business_key")
    column_map              = settings.get("column_map")
    data_type_map           = settings.get("data_type_map")

    return (
        spark.readStream
        .options(**readStreamOptions)
        .table(src_table_name)
    )
