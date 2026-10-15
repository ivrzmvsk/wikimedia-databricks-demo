from pathlib import Path
import re
from types import SimpleNamespace

import pytest


NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"


def test_parameterized_gold_build_reconciles_and_can_be_rerun(spark):
    parameters = {
        "catalog": "spark_catalog",
        "bronze_schema": "bundle_test_bronze",
        "silver_schema": "bundle_test_silver",
        "gold_schema": "bundle_test_gold",
    }
    widgets = SimpleNamespace(
        text=lambda name, default: parameters.setdefault(name, default),
        get=lambda name: parameters[name],
    )
    def local_sql(query):
        # The external Zerobus inbox is also represented with local Parquet.
        query = query.replace("USING DELTA", "USING parquet")
        # Local Spark has no Delta REPLACE TABLE. Execute the same SELECTs
        # against Parquet tables; replacement semantics need a workspace check.
        match = re.search(r"CREATE OR REPLACE TABLE (\S+) AS", query)
        if match:
            spark.sql(f"DROP TABLE IF EXISTS {match[1]}")
            query = query.replace(match[0], f"CREATE TABLE {match[1]} USING parquet AS")
        return spark.sql(query)

    context = {"spark": SimpleNamespace(sql=local_sql),
               "dbutils": SimpleNamespace(widgets=widgets)}

    def run(path):
        source = (NOTEBOOKS / path).read_text()
        exec(compile(source, str(path), "exec"), context)

    try:
        run("config.py")
        run("setup.py")
        run("setup_inbox.py")
        run("setup_gold.py")
        assert spark.table("spark_catalog.bundle_test_gold.fact_wikipedia_edits").count() == 0
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW reference_fixture AS
            SELECT 'enwiki' AS wiki_code, 'en' AS language_code,
                'Wikipedia' AS project_name, 'https://en.wikipedia.org' AS site_url,
                CAST(NULL AS TIMESTAMP) AS __END_AT
        """)
        spark.sql("""
            CREATE TABLE spark_catalog.bundle_test_silver.silver_ref_wikimedia_projects
            USING parquet AS SELECT * FROM reference_fixture
        """)
        spark.sql("""
            CREATE TABLE spark_catalog.bundle_test_silver.silver_wikipedia_edits
            USING parquet AS
            SELECT 1 AS event_id, 'enwiki' AS wiki, true AS bot,
                'editor' AS user, 'Page' AS title,
                TIMESTAMP '2026-10-12 12:00:00' AS event_time,
                TIMESTAMP '2026-10-12 12:01:00' AS silver_ingested_at,
                10 AS length_old, 25 AS length_new, true AS minor
            UNION ALL
            SELECT 2, 'missingwiki', false, 'editor2', 'Other page',
                TIMESTAMP '2026-10-12 12:00:00',
                TIMESTAMP '2026-10-12 12:01:00', 20, 10, false
        """)
        for _ in range(2):
            for path in ("gold/01_dim_date.py", "gold/02_dim_project.py",
                         "gold/03_dim_editor_type.py", "gold/04_fact_wikipedia_edits.py"):
                run(path)
            run("check_reference.py")
            run("check_gold.py")
        facts = spark.table("spark_catalog.bundle_test_gold.fact_wikipedia_edits")
        rows = {row.event_id: row for row in facts.collect()}
        assert len(rows) == 2
        # Setup can be repeated without clearing an existing fact table.
        run("setup_gold.py")
        assert facts.count() == 2
        assert rows[1].byte_change == 15
        assert rows[2].abs_byte_change == 10
        assert rows[1].editor_type_key == 1
        assert rows[2].editor_type_key == 2

        spark.sql("""
            INSERT INTO spark_catalog.bundle_test_gold.fact_wikipedia_edits
            SELECT * FROM spark_catalog.bundle_test_gold.fact_wikipedia_edits
        """)
        with pytest.raises(AssertionError, match="silver_gold_reconciliation"):
            run("check_gold.py")
        run("gold/04_fact_wikipedia_edits.py")

        # A broken fact/dimension relationship must make the workflow fail.
        spark.sql("DROP TABLE spark_catalog.bundle_test_gold.dim_project")
        spark.sql("""
            CREATE TABLE spark_catalog.bundle_test_gold.dim_project
            USING parquet AS SELECT 'invalid-key' AS project_key
        """)
        with pytest.raises(AssertionError, match="project_referential_integrity"):
            run("check_gold.py")
    finally:
        for schema in ("bundle_test_gold", "bundle_test_silver", "bundle_test_bronze"):
            spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
