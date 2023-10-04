# Multiple files - All files moved to one DBFS location and called using folder names

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

laps_schema = StructType(fields=[
    StructField("raceId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=False),
    StructField("lap", IntegerType(), nullable=False),
    StructField("position", IntegerType(), nullable=False),
    StructField("time", StringType(), nullable=True),
    StructField("milliseconds", StringType(), nullable=True)
])

laps_df = spark.read \
    .schema(laps_schema) \
    .csv('dbfs:/FileStore/tables/laptimes')

from pyspark.sql.functions import current_timestamp

laps_df_final = laps_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp())

laps_df_final.write.mode("overwrite").parquet("/dbfs/FileStore/parquet_output/processed/pitstops")
