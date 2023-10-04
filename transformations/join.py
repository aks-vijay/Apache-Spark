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

# full join both the dataframes
df_joined = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, "full") \
    .select(
            df_circuits.name.alias("circuit_name"), 
            df_circuits.location, 
            df_circuits.country, 
            df_races.name.alias("race_name"), 
            df_races.round
            )

# semi join - same as inner join but only returns left dataframe
df_joined = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, "semi") \
    .select(
            df_circuits.name.alias("circuit_name"), 
            df_circuits.location, 
            df_circuits.country
            )

# anti join - similar to semi join but returns unmatched records of all the left dataframe which are not matching with right dataframe
df_joined = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, "anti") 

# cross join
df_circuits.crossJoin(df_races)
