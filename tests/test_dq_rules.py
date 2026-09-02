import json

from tests.stream_test_helpers import bronze_df, valid_event
from wikimedia_stream.dq_rules import compute_dq_flags, filter_bad_records, filter_good_records
from wikimedia_stream.transforms import parse_bronze_to_silver


def parsed_with_dq(spark, event):
    payload = event if isinstance(event, str) else json.dumps(event)
    return compute_dq_flags(parse_bronze_to_silver(bronze_df(spark, [payload])))


def test_valid_event_passes_dq(spark):
    row = parsed_with_dq(spark, valid_event()).collect()[0]

    assert row._dq_passed is True
    assert row._dq_failure_reasons == []


def test_missing_event_id_fails_dq(spark):
    event = valid_event(id=None)

    row = parsed_with_dq(spark, event).collect()[0]

    assert row._dq_passed is False
    assert "valid_event_id" in row._dq_failure_reasons


def test_missing_wiki_domain_fails_dq(spark):
    event = valid_event(wiki="", meta={"id": "event-124", "domain": "", "dt": "2026-09-01T12:00:00Z"})

    row = parsed_with_dq(spark, event).collect()[0]

    assert row._dq_passed is False
    assert "valid_wiki_domain" in row._dq_failure_reasons


def test_invalid_length_fails_dq(spark):
    event = valid_event(length={"old": -1, "new": 10})

    row = parsed_with_dq(spark, event).collect()[0]

    assert row._dq_passed is False
    assert "valid_length" in row._dq_failure_reasons


def test_good_and_bad_filters_split_records(spark):
    source = parse_bronze_to_silver(
        bronze_df(spark, [json.dumps(valid_event()), "{not-json"])
    )
    flagged = compute_dq_flags(source)

    assert filter_good_records(flagged).count() == 1
    assert filter_bad_records(flagged).count() == 1
