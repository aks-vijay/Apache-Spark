input_file = '/FileStore/tables/drivers.json'

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

names_schema = StructType(fields=[
    StructField("forename", StringType(), nullable=True),
    StructField("surname", StringType(), nullable=True)
])

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), nullable=False),
    StructField("driverRef", IntegerType(), nullable=True),
    StructField("code", IntegerType(), nullable=True),
    StructField("dob", DateType(), nullable=True),
    StructField("name", names_schema),
    StructField("nationality", StringType(), nullable=True),
    StructField("number", IntegerType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

drivers_df = spark.read \
                .schema(drivers_schema) \
                .json(input_file)

# transform
from pyspark.sql.functions import current_timestamp, concat, lit, col

drivers_final = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(drivers_df.name.forename, lit(" "), drivers_df.name.surname)) \
    .drop("url")

write_mode = "overwrite"
output_location = '/dbfs/FileStore/parquet_output/processed/drivers'

drivers_final.write.mode(write_mode).parquet(output_location)
