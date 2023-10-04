from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# schema
qualifying_schema = StructType(fields=[
    StructField("qualifyingId", IntegerType(), nullable=False),
    StructField("raceId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=False),
    StructField("constructorId", IntegerType(), nullable=False),
    StructField("number", IntegerType(), nullable=True),
    StructField("position", IntegerType(), nullable=True),
    StructField("q1", StringType(), nullable=True),
    StructField("q2", StringType(), nullable=True),
    StructField("q3", StringType(), nullable=True)
])

# read and transform columns
qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json('dbfs:/FileStore/tables/qualifying/') \
    .withColumnRenamed("qualifyingId", "qualifying_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp())

# load to parquet
qualifying_df.write.mode("overwrite").parquet("/dbfs/FileStore/parquet_output/processed/qualifying")
