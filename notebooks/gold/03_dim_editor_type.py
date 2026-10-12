# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.dim_editor_type AS
SELECT
    1 AS editor_type_key,
    true AS is_bot,
    'Bot' AS editor_type_desc
UNION ALL
SELECT
    2 AS editor_type_key,
    false AS is_bot,
    'Human' AS editor_type_desc
""")
