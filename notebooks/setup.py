# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# External ingest owns this table; Lakeflow owns the downstream bronze_stream.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE}.zerobus_wikimedia_inbox (
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
        f"GRANT SELECT, MODIFY ON TABLE {BRONZE}.zerobus_wikimedia_inbox TO {principal}"
    )
