# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

reference = spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {SILVER}.silver_ref_wikimedia_projects
            WHERE __END_AT IS NULL) AS source_projects,
        (SELECT COUNT(*) FROM {GOLD}.dim_project
            WHERE wiki_code <> 'unknown') AS published_projects,
        (SELECT COUNT(DISTINCT project_key) FROM {GOLD}.dim_project) AS unique_keys,
        (SELECT COUNT(*) FROM {GOLD}.dim_project
            WHERE wiki_code = 'unknown') AS fallback_projects,
        (SELECT COUNT(*) FROM {SILVER}.silver_ref_wikimedia_projects s
            LEFT ANTI JOIN {GOLD}.dim_project p
                ON p.project_key = md5(s.wiki_code)
            WHERE s.__END_AT IS NULL) AS missing_projects
""").first()

if (
    reference["source_projects"] == 0
    or reference["published_projects"] != reference["source_projects"]
    or reference["unique_keys"] != reference["published_projects"] + 1
    or reference["fallback_projects"] != 1
    or reference["missing_projects"] != 0
):
    raise AssertionError(f"Reference publication checks failed: {reference}")

print(f"Reference publication checks passed: {reference}")
