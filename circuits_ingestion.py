from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

#StrucType (Fields = [])

circuits_schema = StructType(
                            fields=[
                                     StructField("circuitID", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", DoubleType(), False),
                            ])

circuits_df = spark.read \
            .option("header", True) \
            .schema(circuits_schema) \
            .csv("/FileStore/tables/circuits-1.csv")

circuits_df.printSchema()

circuits_df.describe().show()

circuits_df_selected_v2 = circuits_df.select("*")

circuits_renamed_df = circuits_df_selected.withColumnRenamed("circuitID","circuit_ID") \
                                        .withColumnRenamed("circuitRef","circuit_Ref") \
                                        .withColumnRenamed("lat","latitude") \
                                        .withColumnRenamed("lng","longitude") \
                                        .withColumnRenamed("alt","altitude") 

from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("Env",lit("Production")) # For literal value --> Wrap the value around lit()


circuits_final_df.write \
.mode("overwrite") \
.parquet('/dbfs:/Users/akshay/desktop')

df = spark.read.parquet('dbfs:/Users/akshay/desktop/')
display(df)
