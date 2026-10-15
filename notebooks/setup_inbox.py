# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

raw_events_table = f"{BRONZE}.{identifier(dbutils.widgets.get('raw_events_table'))}"

# The external producer writes this sole Bronze event table; Lakeflow reads it directly.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {raw_events_table} (
        event_json STRING,
        event_key STRING,
        produced_at BIGINT
    ) USING DELTA
    TBLPROPERTIES ('delta.appendOnly' = 'true')
""")
if ZEROBUS_SERVICE_PRINCIPAL:
    principal = identifier(ZEROBUS_SERVICE_PRINCIPAL)
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {principal}")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {BRONZE} TO {principal}")
    spark.sql(
        f"GRANT SELECT, MODIFY ON TABLE {raw_events_table} TO {principal}"
    )
