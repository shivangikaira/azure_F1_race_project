-- Databricks notebook source
USE CATALOG hive_metastore;

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS hive_metastore.f1_raw.circuits;
CREATE TABLE IF NOT EXISTS hive_metastore.f1_raw.circuits
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING, 
  lat  DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv 
OPTIONS (path "/mnt/kairaudemydb/raw/circuits.csv", header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create races table

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING,
  url STRING)
  USING csv 
  OPTIONS (path "/mnt/kairaudemydb/raw/races.csv",header = true); 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Create constructors file

-- COMMAND ----------

create table if not exists f1_raw.constructors
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
using json
options (path "/mnt/kairaudemydb/raw/constructors.json", header = true)

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT < forename : STRING, surname : STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
using json
options (path "/mnt/kairaudemydb/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results
(
 resultId INT,
 raceId INT,
 driverId INT,
 constructorId INT,
 number INT,
 grid INT,
 position INT,
 positionText STRING,
 points INT,
 laps INT,
 time STRING,
 milliseconds INT,
 fastestLap INT,
 rank INT,
 fastestLapTime STRING,
 fastestLapSpeed FLOAT,
 statusId STRING
)
using json
options (path "/mnt/kairaudemydb/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.RESULTS WHERE FAStestlaptime IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create pit stops table
-- MAGIC - multi line json
-- MAGIC - simple structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops
(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
using json
options (path "/mnt/kairaudemydb/raw/pit_stops.json", multiline true)  

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/lap_times")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/formula1dl/raw/qualifying", multiLine true)
