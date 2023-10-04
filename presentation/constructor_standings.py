# fetch the local variables
%run "../configuration/configuration"

# read parquet file of the grouped data from multiple data models
constructor_standings_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# transform as per presentation layer
from pyspark.sql.functions import sum, count_if

constructor_standings_grouped = constructor_standings_df \
    .groupBy("race_year", "team") \
    .agg(
        sum("points").alias("total_points"),
        count_if(constructor_standings_df.position == 1).alias("wins")
    )

# rank the team based on total points and wins
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc
 
constructor_standings_specification = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_grouped \
    .withColumn("rank", rank().over(constructor_standings_specification))

# write to parquet file 
final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
