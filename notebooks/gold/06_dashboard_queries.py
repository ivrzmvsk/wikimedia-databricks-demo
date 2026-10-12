# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

display(spark.sql(f"""
-- ============================================================================
-- Pipeline Step: Gold Dashboard Queries
-- Description: Optimized analytical queries for Databricks Lakehouse Dashboards.
-- ============================================================================

-- Query 1: Top Languages by Volume & Bot Activity (Parameterized / Filterable)
SELECT
    p.language_name,
    p.project_name,
    SUM(CASE WHEN e.editor_type_desc = 'Bot' THEN 1 ELSE 0 END) AS bot_edits,
    SUM(CASE WHEN e.editor_type_desc = 'Human' THEN 1 ELSE 0 END) AS human_edits,
    COUNT(f.event_id) AS total_edits,
    SUM(f.abs_byte_change) AS total_bytes_altered
FROM {GOLD}.fact_wikipedia_edits f
JOIN {GOLD}.dim_project p ON f.project_key = p.project_key
JOIN {GOLD}.dim_editor_type e ON f.editor_type_key = e.editor_type_key
JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2
ORDER BY total_edits DESC
LIMIT 40
"""))

# COMMAND ----------

display(spark.sql(f"""
-- Query 2: Bot vs Human Edit Behavior (Size & Minor Flag Ratio)
SELECT
    e.editor_type_desc,
    COUNT(f.event_id) AS total_edits,
    ROUND(AVG(f.abs_byte_change), 1) AS avg_byte_change,
    ROUND(SUM(CASE WHEN f.is_minor_edit = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS minor_edit_pct
FROM {GOLD}.fact_wikipedia_edits f
JOIN {GOLD}.dim_editor_type e ON f.editor_type_key = e.editor_type_key
GROUP BY 1
"""))

# COMMAND ----------

display(spark.sql(f"""
-- Query 3: Edits Timeline by Hour/Minute
SELECT
    date_trunc('minute', f.event_time) AS event_minute,
    e.editor_type_desc,
    COUNT(f.event_id) AS edits_count
FROM {GOLD}.fact_wikipedia_edits f
JOIN {GOLD}.dim_editor_type e ON f.editor_type_key = e.editor_type_key
GROUP BY 1, 2
ORDER BY event_minute DESC
LIMIT 60
"""))
