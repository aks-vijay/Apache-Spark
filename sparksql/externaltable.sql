%run "../configuration/configuration"

%python
## read the parquet file
race_results_df = spark.read.parquet(f"{processed_folder_path}/race_results")

## write to table
## format option - write to external path 
race_results_df.write.mode("overwrite") \
    .format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py") \
    .saveAsTable("demo.race_results_python_ext_py")

USE demo;
SHOW TABLES;

USE demo;
DESC EXTENDED demo.race_results_python_ext_py;

DROP TABLE IF EXISTS demo.race_results_python_ext_sql;
-- External table - Location details is MUST
-- Create table using Parquet
-- Even if table is deleted, the data will still be in the DBFS or ADLs location until we delete the data.
-- Table can be re created and ready to use. Data load is not necessary again.
CREATE TABLE demo.race_results_python_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "dbfs:/dbfs/FileStore/parquet_output/presentation/race_results_python_ext_sql"

INSERT INTO demo.race_results_python_ext_sql
SELECT * FROM demo.race_results_python_ext_py WHERE race_year = 2020;

%fs 
ls dbfs:/dbfs/FileStore/parquet_output/presentation/race_results_python_ext_sql/
