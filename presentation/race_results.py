# extract from config file
%run "../configuration/configuration"

# read the transformed parquet files
df_circuits = spark.read.parquet(f"{processed_folder_path}/circuits")
df_drivers = spark.read.parquet(f"{processed_folder_path}/drivers")
df_constructors = spark.read.parquet(f"{processed_folder_path}/constructors")
df_races = spark.read.parquet(f"{processed_folder_path}/races")
df_results = spark.read.parquet(f"{processed_folder_path}/results")

# join the dataframes as per requirement
from pyspark.sql.functions import current_timestamp

df_joined = df_races.join(df_circuits, df_races.circuit_id == df_races.circuit_id, "inner") \
    .join(df_results, df_races.race_id == df_results.race_id, "inner") \
    .join(df_drivers, df_drivers.driver_id == df_results.driver_id, "inner") \
    .join(df_constructors, df_constructors.constructor_id == df_results.constructor_id) \
    .select(df_races.race_year, 
            df_races.name.alias("race_name"),
            df_races.race_timestamp.alias("race_date"),
            df_circuits.location.alias("circuit_location"),
            df_drivers.name.alias("driver_name"),
            df_drivers.number.alias("driver_number"),
            df_drivers.nationality.alias("driver_nationality"),
            df_constructors.name.alias("team"),
            df_results.grid,
            df_results.fastest_lap,
            df_results.time.alias("race time"),
            df_results.points) \
    .withColumn("created_date", current_timestamp())

# write to presentation layer
df_joined.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
