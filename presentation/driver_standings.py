%run "../configuration/configuration"

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

from pyspark.sql.functions import sum, when, count

## count the wins only if position is 1
driver_standings_df = race_results_df \
    .groupBy("race_year","driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(race_results_df.position == 1, True)
        ).alias("wins"))

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# parition by year and order by total points + wins & rank it
# if total_points for a driver is a tie, then need to rank using wins
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# f1_presentation - creating as managed DB
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "dbfs:/FileStore/presentation"

# write the parquet file and save it under f1_presentationdb
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
