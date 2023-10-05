%run "../configuration/configuration"

%python
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

%python
-- use write format as "parquet" and then save the table to demo.race_results_python table
race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

USE demo;
SHOW TABLES;

DESC EXTENDED race_results_python;

USE demo;

CREATE TABLE race_results_sql
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

DROP TABLE IF EXISTS race_results_sql;
