# None of this worked when I tried it. It ran but always pulled zero records.
# Maybe I can figure it out later.

## This is the catchup transform, in case you skipped a couple
## of days and your streaming update has >1 sets of data
## Look in write.py for instructions how to use this
def silver_scd2_catchup_transform(df, settings, spark):
    latest = df.agg({"derived_ingest_time": "max"}).collect()[0][0]
    return df.filter(col("derived_ingest_time") == lit(latest))
    # latest = (
    #     spark.read.table(settings["dst_table_name"])
    #     .agg({"derived_ingest_time": "max"})
    #     .collect()[0][0]
    # )

    # return df.filter(col("derived_ingest_time") > lit(latest))




## Let's you catch up a silver SCD2 table based on streaming
## (Bronze assumed to have duplicates on the business_key, it loops through them)
## This is all untested by the way.
# To use this, set your settings like this
#     "read_function": "functions.stream_read_table",                   # normal readstream
#     "transform_function": "functions.silver_scd2_catchup_transform",  # dummy transforms (real transforms in upsert)
#     "write_function": "functions.stream_scd2_catchup_write",          # Streams entire batch (trigger=availableNow)
#     "upsert_function": "functions.scd2_catchup_outer_upsert",         # outer upsert, loops through times, runs inner upsert one by one
def stream_scd2_catchup_write(df, settings, spark):
    business_key = settings["business_key"]
    ingest_time_column = settings["ingest_time_column"]

    upsert_func = get_function(settings.get("upsert_function"))
    return (
        df.writeStream
        .queryName(settings["dst_table_name"])
        .options(**settings["writeStreamOptions"])
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(settings, spark))
        .outputMode("update")
        .start()
    )

def scd2_catchup_outer_upsert(settings, spark):
    def outer_upsert_fn(df, batchId):
        upsert = scd2_upsert(settings, spark)

        times = (
            df.select("derived_ingest_time")
              .distinct()
              .orderBy("derived_ingest_time")
              .collect()
        )

        for row in times:
            ts = row["derived_ingest_time"]
            batch = df.filter(col("derived_ingest_time") == ts)
            batch = silver_scd2_transform(batch, settings, spark)
            upsert(batch, batchId)

    return outer_upsert_fn