# prepare the data
df_window = race_results_df.where("race_year in (2019, 2020)")

# grouped based on race  year and driver names with required aggregation
demo_grouped_df = df_window \
                    .groupBy("race_year", "driver_name") \
                    .agg(sum("points").alias("sum_of_points"), countDistinct("race_name").alias("number_of_races"))

display(demo_grouped_df.orderBy(demo_grouped_df.race_year.desc()))

## Here we need to one more column called Rank
## For each race_year + Order by the sum_of_points + and give a rank

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank,dense_rank

## For each race_year (parition by race_year) + Order by the sum_of_points desc + and give a rank
driverRankSpecification = Window.partitionBy("race_year").orderBy(desc("sum_of_points"))

## add new column + give a rank over spec defined
demo_grouped_df.withColumn("Rank", rank().over(driverRankSpecification)).show(100)

## requirement is no matter what year - give me the rank based on the points
driverRankSpecWithoutYear = Window.orderBy(desc("sum_of_points"))
demo_grouped_df.withColumn("Rank", rank().over(driverRankSpecWithoutYear)).show(100)
