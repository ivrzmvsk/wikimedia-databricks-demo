from wikimedia_stream.schema import WIKIPEDIA_EVENT_SCHEMA


def test_schema_contains_controlled_evolution_fields():
    field_names = set(WIKIPEDIA_EVENT_SCHEMA.fieldNames())

    assert "meta" in field_names
    assert "_producer" in field_names

    meta_fields = set(WIKIPEDIA_EVENT_SCHEMA["meta"].dataType.fieldNames())
    producer_fields = set(WIKIPEDIA_EVENT_SCHEMA["_producer"].dataType.fieldNames())

    assert "id" in meta_fields
    assert "source" in producer_fields
    assert "producer_timestamp_utc" in producer_fields

