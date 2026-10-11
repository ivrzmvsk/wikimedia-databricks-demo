# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

spark.sql(f"""
-- ============================================================================
-- Pipeline Step: Gold Dimension - dim_date
-- Description: Generates a standard calendar date dimension for time analytics.
-- Dependencies: Autonomous (No external Silver table dependencies).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS {GOLD}
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.dim_date AS
WITH date_sequence AS (
    -- Generates calendar days covering current and historical analysis periods
    SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'), interval 1 day)) AS calendar_date
)
SELECT
    cast(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
    calendar_date,
    year(calendar_date) AS year,
    quarter(calendar_date) AS quarter,
    month(calendar_date) AS month,
    date_format(calendar_date, 'MMMM') AS month_name,
    day(calendar_date) AS day,
    dayofweek(calendar_date) AS day_of_week,
    date_format(calendar_date, 'EEEE') AS day_name,
    CASE
        WHEN dayofweek(calendar_date) IN (1, 7) THEN true
        ELSE false
    END AS is_weekend
FROM date_sequence
""")
