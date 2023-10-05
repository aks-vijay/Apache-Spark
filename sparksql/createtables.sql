CREATE DATABASE IF NOT EXISTS f1_raw;

--Create circuits table
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
  circuitID INT,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "dbfs:/FileStore/tables/circuits-2.csv", header true)

--races table
CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId INT,
  year INT,
  round INT,
  circuitID INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "dbfs:/FileStore/tables/races.csv", header true)

--JSON table
--Constructors table
DROP TABLE IF EXISTS f1_raw.constructors; 
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT, constructorRef string, name string, nationality string, url string
)
USING JSON
OPTIONS (path "dbfs:/FileStore/tables/constructors.json", header true)

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers 
(
  driverID INT,
  driverRef INT,
  code INT,
  dob DATE,
  name STRUCT<forename: STRING, surname: STRING>,
  nationality STRING,
  number STRING,
  url STRING
)
USING JSON
OPTIONS (path "dbfs:/FileStore/tables/drivers.json", header true)

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(
  constructorID INT,
  driverID INT,
  fastestLap INT,
  fastestLapSpeed STRING,
  fastestLapTime STRING,
  grid INT,
  laps INT,
  milliseconds INT,
  number INT,
  points FLOAT,
  position INT,
  positionOrder STRING,
  positionText STRING,
  raceId INT,
  rank INT,
  resultId INT,
  statusId INT,
  time INT
)
USING JSON
OPTIONS (path "dbfs:/FileStore/tables/results.json")

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops
(
  raceId INT,
  driverID INT,
  stop STRING,
  lap STRING,
  time STRING,
  duration STRING,
  milliseconds STRING
)
USING JSON
OPTIONS (path "dbfs:/FileStore/tables/pit_stops.json", multiLine true)

DROP TABLE IF EXISTS f1_raw.laps;

CREATE TABLE IF NOT EXISTS f1_raw.laps
(
  raceId INT,
  driverID INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (path "dbfs:/FileStore/tables/lap_times/");

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
  qualifyingId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "dbfs:/FileStore/tables/qualifying/", multiLine true)
