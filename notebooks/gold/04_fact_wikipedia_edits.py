# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.fact_wikipedia_edits AS
SELECT
    s.event_id,
    cast(date_format(coalesce(s.event_time, s.silver_ingested_at), 'yyyyMMdd') AS INT) AS date_key,

    -- Assign valid project_key, or fallback to the 'unknown' project key
    COALESCE(p.project_key, md5('unknown')) AS project_key,

    CASE
        WHEN s.bot = true THEN 1
        ELSE 2
    END AS editor_type_key,

    s.user,
    s.title,
    s.event_time,
    s.length_old,
    s.length_new,
    (s.length_new - s.length_old) AS byte_change,
    abs(s.length_new - s.length_old) AS abs_byte_change,
    coalesce(s.minor, false) AS is_minor_edit,
    current_timestamp() AS gold_ingested_at
FROM {SILVER}.silver_wikipedia_edits s
LEFT JOIN {GOLD}.dim_project p
    ON md5(coalesce(s.wiki, 'unknown')) = p.project_key
""")
