# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# Raise on failure so the workflow cannot report success after a failed check.
checks = {
    "unique_project_keys": f"""
        SELECT COUNT(*) AS violations FROM (
            SELECT project_key FROM {GOLD}.dim_project
            GROUP BY project_key HAVING COUNT(*) > 1
        )
    """,
    "project_referential_integrity": f"""
        SELECT COUNT(*) AS violations
        FROM {GOLD}.fact_wikipedia_edits f
        LEFT JOIN {GOLD}.dim_project p ON f.project_key = p.project_key
        WHERE p.project_key IS NULL
    """,
    "date_referential_integrity": f"""
        SELECT COUNT(*) AS violations
        FROM {GOLD}.fact_wikipedia_edits f
        LEFT JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
    """,
    "silver_gold_reconciliation": f"""
        SELECT ABS(
            (SELECT COUNT(*) FROM {SILVER}.silver_wikipedia_edits) -
            (SELECT COUNT(*) FROM {GOLD}.fact_wikipedia_edits)
        ) AS violations
    """,
}

failures = {}
for name, query in checks.items():
    violations = spark.sql(query).first()["violations"]
    print(f"{name}: {violations} violations")
    if violations:
        failures[name] = violations

if failures:
    raise AssertionError(f"Gold quality checks failed: {failures}")
