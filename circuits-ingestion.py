
%run "../configuration/configuration"
%run "../configuration/common_functions"

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

# variable defined in %run "../configuration/configuration"
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/circuits-2.csv')

circuits_df_renamed = circuits_df_selected.withColumnRenamed("circuitID", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# function defined in external file - Extracted using %run "../configuration/common_functions"
circuits_final_df = add_ingestion_date(circuits_df_renamed)

circuits_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/circuits')
