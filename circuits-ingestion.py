
# to fetch the variables from other notebook
%run "../configuration/configuration"
%run "../configuration/common_functions"

# widget to add to the notebook -> Which can be used later anywhere
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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

from pyspark.sql.functions import lit

circuits_df_renamed = circuits_df_selected.withColumnRenamed("circuitID", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source", lit(v_data_source)) ## from widget

# function defined in external file - Extracted using %run "../configuration/common_functions"
circuits_final_df = add_ingestion_date(circuits_df_renamed)

circuits_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/circuits')
