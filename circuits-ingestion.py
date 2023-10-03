from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[
    StructField("circuitID", IntegerType(), nullable=False),
    StructField("circuitRef", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("location",  StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lng", DoubleType(), nullable=True),
    StructField("alt", IntegerType(), nullable=True),
    StructField("url", StringType(), nullable=True)
    ])

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv('/FileStore/tables/circuits-2.csv')

circuits_df_renamed = circuits_df_selected.withColumnRenamed("circuitID", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_df_renamed.withColumn("ingestion_date", current_timestamp()) 

circuits_final_df.write.mode("overwrite").parquet('/dbfs/FileStore/parquet_output/processed/circuits')
