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

from wikimedia_stream.transforms import decode_zerobus_events


CATALOG_NAME = spark.conf.get("wikimedia.catalog", "dbr_dev")
BRONZE_SCHEMA_NAME = spark.conf.get("wikimedia.bronze_schema", "wikimediademo_bronze")
BRONZE_STREAM_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA_NAME}.bronze_stream"
ZEROBUS_INBOX_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA_NAME}.zerobus_wikimedia_inbox"


# Keep the existing table and its history. A distinct flow name gives the Delta
# source a fresh checkpoint instead of reusing the former Kafka checkpoint.
dp.create_streaming_table(
    name=BRONZE_STREAM_TABLE,
    comment="Raw Wikimedia recent-change events ingested through Zerobus.",
    expect_all={"bronze_payload_present": "event_json IS NOT NULL"},
)


@dp.append_flow(target=BRONZE_STREAM_TABLE, name="zerobus_wikimedia_ingest")
def bronze_stream():
    return decode_zerobus_events(spark.readStream.table(ZEROBUS_INBOX_TABLE))
