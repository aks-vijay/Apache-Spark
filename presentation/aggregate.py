#############################################################################################
# Notes:
# Aggregation can be added only to objects. If its converted to Df, then its not possible
# Dataframe.select(sum()) --> Syntax for returning one. Here select will return an object
# Dataframe.groupBy(column name).sum() -> Group by and then aggregate (Reverse as SQL)
# Dataframe.groupBy(agg(sum(col1), sum(col2))) -> For multiple columns
############################################################################################

# load variables
%run "../configuration/configuration"

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# filter only required data
race_results_filtered = race_results_df.where("race_year == 2020")

# basic aggregations on the whole dataset
from pyspark.sql.functions import count, countDistinct, sum

race_results_filtered.select(countDistinct("race_name")).show()
race_results_filtered.select(sum("points")).show()

# basic aggregations for one driver
race_results_filtered.where("driver_name == 'Lewis Hamilton' ").select(sum("points"),countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "Sum of points by Lewis Hamilton") \
    .withColumnRenamed("count(DISTINCT race_name)", "Distinct Race names") \
    .show()

# group by - 1 aggregation
race_results_filtered.groupBy("driver_name").sum("points").show()

# group by - 2 aggregation
# reason for agg -> if aggregate functions like sum is added it's converted to dataframe and count cannot be added directly
# Need to be an object to add aggregation
# group by and add multiple aggregation
race_results_filtered.groupBy("driver_name") \
    .agg(
        sum("points").alias("Sum of points"),
        countDistinct("race_name").alias("Distinct Race names")
    ).show()

# group by based on multiple columns and add multiple aggregation
race_results_filtered.groupBy("driver_name", "circuit_location") \
    .agg(
        sum("points").alias("Sum of points"),
        countDistinct("race_name").alias("Distinct Race names")
    ).show()
