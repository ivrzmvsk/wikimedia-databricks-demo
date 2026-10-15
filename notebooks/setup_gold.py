# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# Create only on first installation. Repeated setup must never replace fact data.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.fact_wikipedia_edits (
    event_id BIGINT,
    date_key INT,
    project_key STRING,
    editor_type_key INT,
    user STRING,
    title STRING,
    event_time TIMESTAMP,
    length_old BIGINT,
    length_new BIGINT,
    byte_change BIGINT,
    abs_byte_change BIGINT,
    is_minor_edit BOOLEAN,
    gold_ingested_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %run ./gold/05_unity_catalog_governance
