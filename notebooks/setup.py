# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
