from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

def merge_upsert(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in mergeKeys])

        microBatchDF.createOrReplaceTempView("microBatchDF")

        microBatchDF.sparkSession.sql(f"""
            MERGE INTO {destinationTable} AS target
            USING microBatchDF AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    return _do_upsert

def merge_upsert_delete(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in mergeKeys])

        microBatchDF.createOrReplaceTempView("microBatchDF")

        microBatchDF.sparkSession.sql(f"""
            MERGE INTO {destinationTable} AS target
            USING microBatchDF AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)

    return _do_upsert

def spark_upsert(spark, sourcePK, destinationTable, destinationPK):
    def _do_upsert(microBatchDF, batchId):
        microBatchDF = (
            microBatchDF
            .withColumn("row_number", row_number().over(Window.partitionBy(sourcePK).orderBy(col("ingest_time").desc())))
            .filter(col("row_number") == 1)
            .drop("row_number")
        )

        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forName(spark, destinationTable)
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), f"s.{sourcePK} = d.{destinationPK}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return _do_upsert

def spark_upsert_delete(spark, mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):        
        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forName(spark, destinationTable)

        merge_condition = " AND ".join([f"s.{col} = d.{col}" for col in mergeKeys])
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    return _do_upsert














