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

from wikimedia_stream.transforms import add_bronze_metadata, decode_kafka_events


CATALOG_NAME = spark.conf.get("wikimedia.catalog", "dbr_dev")
BRONZE_SCHEMA_NAME = spark.conf.get("wikimedia.bronze_schema", "wikimediademo_bronze")
EVENTHUB_NAMESPACE = spark.conf.get("wikimedia.eventhub.namespace")
EVENTHUB_NAME = spark.conf.get("wikimedia.eventhub.name")
SECRET_SCOPE_NAME = spark.conf.get("wikimedia.secret.scope")
EVENTHUB_SECRET_NAME = spark.conf.get("wikimedia.eventhub.secret_name")
BRONZE_STREAM_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA_NAME}.bronze_stream"


def eventhub_kafka_options():
    connection_string = dbutils.secrets.get(
        scope=SECRET_SCOPE_NAME,
        key=EVENTHUB_SECRET_NAME,
    )
    bootstrap_servers = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
    sasl_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="$ConnectionString" '
        f'password="{connection_string}";'
    )

    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": EVENTHUB_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": sasl_config,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


@dp.table(
    name=BRONZE_STREAM_TABLE,
    comment="Raw Wikimedia recent-change events from Azure Event Hubs.",
)
@dp.expect_all(
    {
        "bronze_payload_present": "event_json IS NOT NULL",
        "bronze_topic_present": "kafka_topic IS NOT NULL",
        "bronze_partition_present": "kafka_partition IS NOT NULL",
        "bronze_offset_present": "kafka_offset IS NOT NULL",
    }
)
def bronze_stream():
    raw_stream = spark.readStream.format("kafka").options(
        **eventhub_kafka_options()
    ).load()

    return add_bronze_metadata(decode_kafka_events(raw_stream))
