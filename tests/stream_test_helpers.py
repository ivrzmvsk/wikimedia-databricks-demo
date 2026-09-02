import json

from pyspark.sql import functions as F


def bronze_df(spark, payloads):
    rows = []
    for index, payload in enumerate(payloads):
        rows.append(
            (
                payload,
                "ivanrazumovskyi_evh",
                0,
                index,
                "2026-09-01 12:00:00",
                "azure_eventhub_wikimedia_recentchange",
                "2026-09-01 12:00:01",
                "2026-09-01",
            )
        )

    return spark.createDataFrame(
        rows,
        [
            "event_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_enqueued_at",
            "_source",
            "_ingested_at",
            "_load_date",
        ],
    ).select(
        "event_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        F.col("kafka_enqueued_at").cast("timestamp").alias("kafka_enqueued_at"),
        "_source",
        F.col("_ingested_at").cast("timestamp").alias("_ingested_at"),
        F.col("_load_date").cast("date").alias("_load_date"),
    )


def valid_event(**overrides):
    event = {
        "id": 123,
        "type": "edit",
        "namespace": 0,
        "title": "Test page",
        "title_url": "https://en.wikipedia.org/wiki/Test_page",
        "comment": "demo edit",
        "timestamp": 1788264000,
        "user": "ExampleUser",
        "bot": False,
        "minor": True,
        "patrolled": True,
        "server_name": "en.wikipedia.org",
        "wiki": "enwiki",
        "meta": {
            "id": "event-123",
            "domain": "en.wikipedia.org",
            "dt": "2026-09-01T12:00:00Z",
        },
        "length": {"old": 100, "new": 140},
        "_producer": {
            "source": "wikimedia-recentchange",
            "producer_timestamp_utc": "2026-09-01T12:00:02+00:00",
        },
    }
    event.update(overrides)
    return event


def valid_event_json(**overrides):
    return json.dumps(valid_event(**overrides))

