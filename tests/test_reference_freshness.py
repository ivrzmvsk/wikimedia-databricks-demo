from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("hours_old,wiki_code,expected", [
    (1, "enwiki", 0),
    (49, "enwiki", 1),
    (1, "unknown", 1),
    (None, None, 1),
])
def test_reference_freshness_detects_stale_missing_and_fallback_only_snapshots(
    spark, hours_old, wiki_code, expected
):
    rows = [] if hours_old is None else [
        (wiki_code, datetime.now(timezone.utc) - timedelta(hours=hours_old))
    ]
    spark.createDataFrame(rows, "wiki_code STRING, gold_updated_at TIMESTAMP").createOrReplaceTempView(
        "reference_freshness_fixture"
    )
    try:
        alerts = yaml.safe_load((ROOT / "resources/alerts.yml").read_text())["resources"]["alerts"]
        query = alerts["wikimedia_reference_freshness"]["query_text"].replace(
            "`${var.catalog}`.`${var.gold_schema}`.dim_project", "reference_freshness_fixture"
        )
        assert spark.sql(query).first()["stale_reference"] == expected
    finally:
        spark.catalog.dropTempView("reference_freshness_fixture")
