

def edsm_gold_read(spark, settings):
    return spark.read.table(settings["src_table_name"])

