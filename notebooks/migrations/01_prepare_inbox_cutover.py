# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# One-time manual migration, never a regular job task. Use a fresh backup schema
# for each migration attempt; existing snapshots are deliberately not replaced.
dbutils.widgets.text("backup_schema", "")
BACKUP = f"{CATALOG}.{identifier(dbutils.widgets.get('backup_schema'))}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BACKUP}")

for name, source in (
    ("bronze_stream", f"{BRONZE}.bronze_stream"),
    ("silver_wikipedia_edits", f"{SILVER}.silver_wikipedia_edits"),
    ("silver_wikipedia_edits_quarantine", f"{SILVER}.silver_wikipedia_edits_quarantine"),
):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {BACKUP}.{name} USING DELTA AS SELECT * FROM {source}")

# Preserve exact raw JSON, including rejected/legacy events. The anti join makes
# repeating this step safe; Silver remains responsible for event-key deduplication.
spark.sql(f"""
    INSERT INTO {BRONZE}.zerobus_wikimedia_inbox (event_json, event_key, produced_at)
    SELECT b.event_json,
        coalesce(get_json_object(b.event_json, '$.meta.id'),
                 sha2(coalesce(b.event_json, ''), 256)),
        unix_micros(coalesce(b._ingested_at, current_timestamp()))
    FROM {BACKUP}.bronze_stream b
    LEFT ANTI JOIN {BRONZE}.zerobus_wikimedia_inbox i
        ON b.event_json <=> i.event_json
""")

missing = spark.sql(f"""
    SELECT COUNT(*) AS missing
    FROM {BACKUP}.bronze_stream b
    LEFT ANTI JOIN {BRONZE}.zerobus_wikimedia_inbox i
        ON b.event_json <=> i.event_json
""").first()["missing"]
if missing:
    raise AssertionError(f"Inbox cutover blocked: {missing} historical payloads missing")

print("History copied. Deploy the direct-inbox pipeline, then explicitly refresh")
print("only Silver edits and quarantine. Verify coverage before retiring bronze_stream.")
