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

##############################################
Explanation:
# Window Specification (driverRankSpecification):

# Window.partitionBy("race_year"): This part defines the window partitions. The data is divided into partitions based on the values in the "race_year" column. This means that window functions will be applied separately to each partition.
# .orderBy(desc("sum_of_points")): This part specifies the ordering within each partition. The data within each partition is sorted in descending order based on the "sum_of_points" column.
# Window Function (rank()):

# The rank() function is a window function that assigns a rank to each row within a partition based on the order specified in the window specification.
# In this case, it will assign a rank to each row within each "race_year" partition based on the descending order of "sum_of_points."
# Adding a New Column (withColumn):

# demo_grouped_df.withColumn("Rank", rank().over(driverRankSpecification)): This line adds a new column named "Rank" to the DataFrame demo_grouped_df. The values in this column will be the ranks assigned by the rank() function based on the window specification.
# Showing the Result (show(100)):

# This displays the first 100 rows of the resulting DataFrame with the newly added "Rank" column.
# Note:
# The rank() function is used here, which means if there are ties (rows with the same "sum_of_points"), those rows will receive the same rank, and the next rank will be skipped.
# If you want to handle ties differently, you might consider using dense_rank() instead of rank(). dense_rank() will not skip ranks for tied values.
