# schema - DDL format
constructor_schema = "constructorId INT, constructorRef string, name string, nationality string, url string"

constructor = spark.read \
                .schema(constructor_schema) \
                .json('/FileStore/tables/constructors.json')

# drop unwanted column
constructor_dropped_df = constructor.drop(constructor["url"])

# rename column
from pyspark.sql.functions import current_timestamp

constructor_final_df = constructor_dropped_df \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumnRenamed("constructorRef", "constructor_ref") \
                        .withColumn("ingestion_date", current_timestamp())

constructor_final_df.write.mode("overwrite").parquet('/dbfs/FileStore/parquet_output/processed/constructors')
