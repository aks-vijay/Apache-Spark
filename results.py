# set local variables
import_file = '/FileStore/tables/results.json'
write_mode = "overwrite"
output_location = '/dbfs/FileStore/parquet_output/processed/results'
parition = "race_id"

# schema
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

results_schema = StructType(fields=[
    StructField("constructorId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=False),
    StructField("fastestLap", IntegerType(), nullable=True),
    StructField("fastestLapSpeed", StringType(), nullable=True),
    StructField("fastestLapTime", StringType(), nullable=True),
    StructField("grid", IntegerType(), nullable=False),
    StructField("laps", IntegerType(), nullable=False),
    StructField("milliseconds", IntegerType(), nullable=True),
    StructField("number", IntegerType(), nullable=True),
    StructField("points", FloatType(), nullable=False),
    StructField("position", IntegerType(), nullable=True),
    StructField("positionOrder", IntegerType(), nullable=False),
    StructField("positionText", StringType(), nullable=False),
    StructField("raceId", IntegerType(), nullable=False),
    StructField("rank", IntegerType(), nullable=True),
    StructField("resultId", IntegerType(), nullable=False),
    StructField("statusId", IntegerType(), nullable=False),
    StructField("time", StringType(), nullable=True)
])

# read data
results = spark.read \
    .schema(results_schema) \
    .json(import_file)

# transform
from pyspark.sql.functions import current_timestamp

results_final = results.withColumnRenamed("resultId", "results_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("ingestion_date", current_timestamp())

# load
results_final.write.mode(write_mode).partitionBy(parition).parquet(output_location)
