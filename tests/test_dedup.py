import json

from tests.stream_test_helpers import bronze_df, valid_event
from wikimedia_stream.dedup import add_dedup_key, deduplicate_records
from wikimedia_stream.transforms import parse_bronze_to_silver


def test_dedup_key_prefers_meta_id(spark):
    df = add_dedup_key(
        parse_bronze_to_silver(bronze_df(spark, [json.dumps(valid_event())]))
    )

    row = df.collect()[0]

    assert row._wikimedia_event_key == "event-123"


def test_dedup_key_falls_back_to_content_hash(spark):
    event = valid_event()
    event["meta"].pop("id")
    df = add_dedup_key(
        parse_bronze_to_silver(bronze_df(spark, [json.dumps(event)]))
    )

    row = df.collect()[0]

    assert row._wikimedia_event_key is not None
    assert row._wikimedia_event_key != ""
    assert row._wikimedia_event_key != "event-123"


def test_content_dedup_removes_replayed_business_event(spark):
    payload = json.dumps(valid_event())
    source = bronze_df(spark, [payload, payload])
    keyed = add_dedup_key(parse_bronze_to_silver(source))

    deduped = deduplicate_records(keyed)

    assert keyed.count() == 2
    assert deduped.count() == 1


def test_malformed_payloads_get_distinct_fallback_keys(spark):
    source = bronze_df(spark, ["{not-json", "{also-not-json"])
    keyed = add_dedup_key(parse_bronze_to_silver(source))

    keys = [row._wikimedia_event_key for row in keyed.collect()]

    assert len(keys) == 2
    assert keys[0] != keys[1]
