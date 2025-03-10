-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;