from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

## read csv
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

races = spark.read \
    .schema(races_schema) \
    .option("header", True) \
    .csv('/FileStore/tables/races.csv')

## Select only required columns
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat
races_selected = races.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

## transform some columns
races_transformed = races_selected.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date", current_timestamp())

## select required columns
races_transformed_final = races_transformed.select(
                                                    col("race_id"),
                                                    col("race_year"),
                                                    col("round"),
                                                    col("circuit_id"),
                                                    col("name"),
                                                    col("race_timestamp"),
                                                    col("ingestion_date")
                                                    )

## write to parquet
races_transformed_final.write.mode("overwrite").parquet('/dbfs/FileStore/parquet_output/processed/races')
