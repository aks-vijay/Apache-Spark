# import sql types for schema of the dataset
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

races_df = spark.read \
            .option ("header", True) \
            .schema(races_schema) \
            .csv('/FileStore/tables/races.csv')
            
            
from pyspark.sql.functions import col

races_df_selected = races_df.select(col("raceId"), col("year"), col("round"), col("circuitID"), col("name"), col("date"), col("time"))

races_df_renamed = races_df_selected \
.withColumnRenamed("raceID", "race_ID") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitID", "circuit_ID") 

# import timestamp and literal packages
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

races_df_timestamp = races_df_renamed \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("ingestion_date", current_timestamp())

races_df_final = races_df_timestamp.select(col("race_ID"), col("race_year"), col("round"), col("circuit_ID"), col("name"), col("race_timestamp"), col("ingestion_date"))

races_df_final.write \
.mode("overwrite") \
.partitionBy('race_year') \
.parquet('dbfs:/FileStore/tables/')
