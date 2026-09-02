SELECT COUNT(*) AS quarantine_records
FROM dbr_dev.wikimediademo_silver.silver_wikipedia_edits_quarantine
WHERE silver_ingested_at >= current_timestamp() - INTERVAL 1 HOURS;
