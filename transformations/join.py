# fetch the processed folder path
%run "../configuration/configuration"

# import from ingested parquet files
df_circuits = spark.read.parquet(f"{processed_folder_path}/circuits").where("circuit_id <= 70")
df_races = spark.read.parquet(f"{processed_folder_path}/races").where("race_year == 2019")

# inner join both the dataframes
df_joined = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, "inner") \
    .select(
            df_circuits.name.alias("circuit_name"), 
            df_circuits.location, 
            df_circuits.country, 
            df_races.name.alias("race_name"), 
            df_races.round
            )

# left join both the dataframes
df_joined = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, "left") \
    .select(
            df_circuits.name.alias("circuit_name"), 
            df_circuits.location, 
            df_circuits.country, 
            df_races.name.alias("race_name"), 
            df_races.round
            )

# right join both the dataframes
df_joined = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, "right") \
    .select(
            df_circuits.name.alias("circuit_name"), 
            df_circuits.location, 
            df_circuits.country, 
            df_races.name.alias("race_name"), 
            df_races.round
            )

