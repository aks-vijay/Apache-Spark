%run "../configuration/configuration"
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# Local Temp View
# Cannot be accessed by other notebooks
# If cluster goes down, will get lost

race_results_df.createOrReplaceTempView("vw_race_results")

# using sql magic commands
%sql 
SELECT COUNT(*) FROM vw_race_results WHERE race_year = 2020

# spark syntax
race_results_2019 = spark.sql(f"""
          SELECT * 
          FROM vw_race_results 
          WHERE race_year = {race_year}
          """)
##################
Global Temp View
##################
# Can access from more than one notebook
# Only if cluster goes down, we can't access it

race_results_df.createOrReplaceGlobalTempView("gvw_race_results")

# sql - magic commands
%sql
SHOW TABLES IN global_temp;

-- cant access directly as temp view because global temp view is stored in global_temp database
-- provide fully defined path

# sql magic command
%sql
SELECT * FROM global_temp.gvw_race_results

# spark syntax
spark.sql("""
          SELECT * FROM global_temp.gvw_race_results
          """)
DataFrame[race_year: int, race_name: string, race_date: timestamp, circuit_location: string, driver_name: string, driver_number: int, driver_nationality: string, team: string, grid: int, fastest_lap: int, race time: string, points: float, position: int, created_date: timestamp]
