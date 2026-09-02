import json

from chispa.dataframe_comparer import assert_df_equality

from tests.stream_test_helpers import bronze_df, valid_event
from wikimedia_stream.transforms import parse_bronze_to_silver


def test_parse_valid_event_with_producer_metadata(spark):
    source = bronze_df(spark, [json.dumps(valid_event())])

    row = parse_bronze_to_silver(source).collect()[0]

    assert row.event_id == 123
    assert row.editor_type == "human"
    assert row.byte_change == 40
    assert row.abs_byte_change == 40
    assert row.producer_source == "wikimedia-recentchange"
    assert row.producer_timestamp_utc is not None
    assert row._parse_error is False


def test_parse_old_payload_without_producer_metadata(spark):
    event = valid_event()
    event.pop("_producer")
    source = bronze_df(spark, [json.dumps(event)])

    row = parse_bronze_to_silver(source).collect()[0]

    assert row.event_id == 123
    assert row.producer_source is None
    assert row.producer_timestamp_utc is None
    assert row._parse_error is False


def test_parse_bot_event_classification(spark):
    source = bronze_df(spark, [json.dumps(valid_event(bot=True))])

    row = parse_bronze_to_silver(source).collect()[0]

    assert row.editor_type == "bot"


def test_parse_projection_matches_expected_columns(spark):
    source = bronze_df(spark, [json.dumps(valid_event())])

    actual = parse_bronze_to_silver(source).select(
        "event_id",
        "event_type",
        "wiki",
        "domain",
        "editor_type",
        "byte_change",
        "abs_byte_change",
    )
    expected = spark.createDataFrame(
        [(123, "edit", "enwiki", "en.wikipedia.org", "human", 40, 40)],
        [
            "event_id",
            "event_type",
            "wiki",
            "domain",
            "editor_type",
            "byte_change",
            "abs_byte_change",
        ],
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_parse_malformed_json_sets_parse_error(spark):
    source = bronze_df(spark, ["{not-json"])

    row = parse_bronze_to_silver(source).collect()[0]

    assert row._parse_error is True
    assert row.event_id is None
