# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# Staleness is monitored separately; an API outage must not block event processing
# while a usable project dimension exists.
try:
    dimensions = spark.sql(f"""
        SELECT
            (SELECT COUNT(*) FROM {GOLD}.dim_date) AS dates,
            (SELECT COUNT(*) FROM {GOLD}.dim_editor_type) AS editor_types,
            (SELECT COUNT(*) FROM {GOLD}.dim_project
                WHERE wiki_code <> 'unknown') AS projects,
            (SELECT COUNT(*) FROM {GOLD}.dim_project
                WHERE wiki_code = 'unknown') AS fallback_projects
    """).first()
except Exception as error:
    raise RuntimeError(
        "Gold dimensions are unavailable. Run wikimedia_setup "
        "and run wikimedia_reference before the event job; check table permissions."
    ) from error

missing_dimensions = any(
    dimensions[name] == 0 for name in ("dates", "editor_types", "projects")
)
if missing_dimensions or dimensions["fallback_projects"] != 1:
    raise RuntimeError(
        "Gold dimensions are empty or missing their unique fallback project. "
        "Run wikimedia_setup and run wikimedia_reference before the event job."
    )

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
