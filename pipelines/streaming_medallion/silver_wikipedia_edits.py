import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def add_repo_src_to_path() -> None:
    repo_root = spark.conf.get("wikimedia.repo_root", "")
    candidates = []

    if repo_root:
        candidates.append(Path(repo_root) / "src")

    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2] / "src")

    candidates.extend(
        [
            Path.cwd() / "src",
            Path.cwd().parent / "src",
        ]
    )

    for candidate in candidates:
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.append(candidate_str)


add_repo_src_to_path()

from wikimedia_stream.dedup import add_dedup_key, deduplicate_stream_records
from wikimedia_stream.dq_rules import DQ_RULES, compute_dq_flags
from wikimedia_stream.transforms import parse_bronze_to_silver


CATALOG_NAME = spark.conf.get("wikimedia.catalog", "dbr_dev")
BRONZE_SCHEMA_NAME = spark.conf.get("wikimedia.bronze_schema", "wikimediademo_bronze")
SILVER_SCHEMA_NAME = spark.conf.get("wikimedia.silver_schema", "wikimediademo_silver")
BRONZE_STREAM_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA_NAME}.bronze_stream"
SILVER_EDITS_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA_NAME}.silver_wikipedia_edits"
SILVER_QUARANTINE_TABLE = (
    f"{CATALOG_NAME}.{SILVER_SCHEMA_NAME}.silver_wikipedia_edits_quarantine"
)


@dp.temporary_view(name="silver_wikipedia_edits_staging")
def silver_wikipedia_edits_staging():
    bronze = dp.read_stream(BRONZE_STREAM_TABLE)
    return compute_dq_flags(add_dedup_key(parse_bronze_to_silver(bronze)))


@dp.table(
    name=SILVER_EDITS_TABLE,
    comment="Clean, parsed, deduplicated Wikimedia recent-change events.",
)
@dp.expect_all_or_drop(DQ_RULES)
def silver_wikipedia_edits():
    return deduplicate_stream_records(dp.read_stream("silver_wikipedia_edits_staging"))


@dp.table(
    name=SILVER_QUARANTINE_TABLE,
    comment="Wikimedia stream records rejected by Silver data-quality rules.",
)
def silver_wikipedia_edits_quarantine():
    return (
        dp.read_stream("silver_wikipedia_edits_staging")
        .filter("_dq_passed = false")
        .withColumn("_quality_status", F.lit("quarantined"))
    )
