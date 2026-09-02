import sys
from pathlib import Path
from pyspark import pipelines as dp

def add_repo_src_to_path() -> None:
    repo_root = spark.conf.get("wikimedia.repo_root", "")
    candidates = []

    if repo_root:
        candidates.append(Path(repo_root) / "src")
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2] / "src")

    candidates.extend([Path.cwd() / "src", Path.cwd().parent / "src"])

    for candidate in candidates:
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.append(candidate_str)

add_repo_src_to_path()

from wikimedia_batch.dq_rules import BATCH_DQ_RULES, compute_dq_flags
from pyspark.sql import functions as F

CATALOG_NAME = spark.conf.get("wikimedia.catalog", "dbr_dev")
BRONZE_SCHEMA_NAME = spark.conf.get("wikimedia.bronze_schema", "wikimediademo_bronze")
SILVER_SCHEMA_NAME = spark.conf.get("wikimedia.silver_schema", "wikimediademo_silver")

BRONZE_REFERENCE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA_NAME}.ref_wikimedia_projects"
SILVER_REFERENCE_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA_NAME}.silver_ref_wikimedia_projects"
SILVER_QUARANTINE_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA_NAME}.silver_ref_quarantine"

@dp.temporary_view(name="silver_ref_staging")
def silver_ref_staging():
    raw_stream = spark.readStream.option("ignoreChanges", "true").table(BRONZE_REFERENCE_TABLE)
    return compute_dq_flags(raw_stream)


@dp.table(
    name=SILVER_QUARANTINE_TABLE,
    comment="Wikimedia reference records rejected by Silver data-quality rules."
)
def silver_ref_quarantine():
    return (
        dp.read_stream("silver_ref_staging")
        .filter("_dq_passed = false")
        .withColumn("_quality_status", F.lit("quarantined"))
    )


@dp.view(name="silver_ref_clean_stream")
@dp.expect_all_or_drop(BATCH_DQ_RULES)
def silver_ref_clean_stream():
    return dp.read_stream("silver_ref_staging")


dp.create_streaming_table(
    name=SILVER_REFERENCE_TABLE,
    comment="SCD Type 2 dimension table for Wikimedia reference data."
)

dp.create_auto_cdc_flow(
    target=SILVER_REFERENCE_TABLE,
    source="silver_ref_clean_stream",
    keys=["wiki_code"],
    sequence_by="_ingest_timestamp",
    track_history_column_list=["project_name", "language_code", "site_url"],
    stored_as_scd_type=2
)