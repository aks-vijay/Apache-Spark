# set local variables
import_file = '/FileStore/tables/pit_stops.json'
write_mode = "overwrite"
output_location = '/dbfs/FileStore/parquet_output/processed/pitstops'

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

pitstops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=True),
    StructField("stop", StringType(), nullable=True),
    StructField("lap", StringType(), nullable=True),
    StructField("time", StringType(), nullable=True),
    StructField("duration", StringType(), nullable=True),
    StructField("milliseconds", StringType(), nullable=True)
])

# file is multi line json, need to mention explicitly
pitstops_df = spark.read \
    .schema(pitstops_schema) \
    .option("multiLine", True) \
    .json(import_file) 

# transform
from pyspark.sql.functions import current_timestamp

pitstops_final = pitstops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingested_date", current_timestamp())

# load
pitstops_final.write.mode(write_mode).parquet(output_location)
