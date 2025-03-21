-- Databricks notebook source
USE CATALOG hive_metastore

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/formula1dl/presentation"