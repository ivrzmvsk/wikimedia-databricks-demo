import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from producer.zerobus import producer
from tests.stream_test_helpers import valid_event
from wikimedia_stream.dq_rules import compute_dq_flags
from wikimedia_stream.transforms import decode_zerobus_events, parse_bronze_to_silver


ROOT = Path(__file__).resolve().parents[1]


def record():
    return producer.normalize(valid_event())


def test_zerobus_preserves_raw_payload_and_stable_source_id():
    event = valid_event()
    event.pop("_producer", None)
    normalized = producer.normalize(event)
    payload = json.loads(normalized["event_json"])
    assert normalized["event_key"] == event["meta"]["id"]
    assert normalized["produced_at"] > 0
    assert payload["_producer"]["source"] == "wikimedia-recentchange"
    assert {key: value for key, value in payload.items() if key != "_producer"} == event
    assert "_producer" not in event
    producer.validate_records([normalized])


def test_zerobus_filters_canaries_and_rejects_missing_source_ids():
    event = valid_event()
    event["meta"]["domain"] = "canary"
    assert producer.normalize(event) is None
    event = valid_event()
    del event["meta"]["id"]
    with pytest.raises(ValueError, match="meta.id"):
        producer.normalize(event)


@pytest.mark.parametrize("event_type", ["new", "log", "categorize", "external", None])
def test_zerobus_skips_non_edit_events(event_type):
    event = valid_event()
    event["type"] = event_type
    assert producer.normalize(event) is None


def test_zerobus_capture_counts_only_edits(monkeypatch):
    events = []
    for index, event_type in enumerate(["new", "edit", "log", "edit"]):
        event = valid_event()
        event["type"] = event_type
        event["meta"]["id"] = str(index)
        events.append(SimpleNamespace(type="message", data=json.dumps(event)))
    source = Mock()
    source.__enter__ = Mock(return_value=events)
    source.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(producer, "EventSource", Mock(return_value=source))
    records = producer._read_capture(2, 120, producer.SOURCE)
    assert [item["event_key"] for item in records] == ["1", "3"]


def test_zerobus_rejects_non_edit_replay_before_sending(tmp_path, monkeypatch):
    item = record()
    payload = json.loads(item["event_json"])
    payload["type"] = "log"
    item["event_json"] = json.dumps(payload)
    path = tmp_path / "old-capture.json"
    path.write_text(json.dumps({
        "target": "ivan_personal", "table": producer.table_name("ivan_personal"),
        "records": [item],
    }))
    monkeypatch.setattr(producer, "credentials", Mock(return_value=("id", "secret")))
    sender = Mock()
    monkeypatch.setattr(producer, "send", sender)
    with pytest.raises(ValueError, match="type='edit'"):
        producer.main(["--replay", "--manifest", str(path)])
    sender.assert_not_called()


@pytest.mark.parametrize("payload", [None, [], {}, {"meta": []}])
def test_zerobus_rejects_malformed_source_payloads(payload):
    with pytest.raises(ValueError):
        producer.normalize(payload)


def test_zerobus_credentials_are_explicit_and_isolated(tmp_path, monkeypatch):
    env = tmp_path / ".env.personal"
    env.write_text("ZEROBUS_CLIENT_ID=personal\nZEROBUS_CLIENT_SECRET='literal-${VALUE}'\n")
    monkeypatch.setenv("ZEROBUS_CLIENT_ID", "other-workspace")
    monkeypatch.setenv("ZEROBUS_CLIENT_SECRET", "other-secret")
    assert producer.credentials(env) == ("personal", "literal-${VALUE}")
    env.write_text("ZEROBUS_CLIENT_ID=personal\n")
    with pytest.raises(ValueError, match="ZEROBUS_CLIENT_SECRET"):
        producer.credentials(env)


def test_zerobus_targets_match_bundle_catalogs_and_are_not_lab11_tables():
    assert producer.table_name("ivan_personal", "demo_bronze") == (
        "workspace.demo_bronze.zerobus_wikimedia_inbox"
    )
    assert producer.table_name("azure_trial_2", "demo_bronze") == (
        "dbr_dev.demo_bronze.raw_events"
    )
    with pytest.raises(ValueError, match="identifier"):
        producer.table_name("ivan_personal", "schema; DROP TABLE x")


def test_zerobus_send_waits_for_flush_and_closes(monkeypatch):
    stream = Mock()
    sdk = Mock()
    sdk.create_stream.return_value = stream
    constructor = Mock(return_value=sdk)
    monkeypatch.setattr(producer, "ZerobusSdk", constructor)
    item = record()
    report = producer.send([item, item], "ivan_personal", "demo_bronze", ("id", "secret"))
    constructor.assert_called_once_with(
        producer.TARGETS["ivan_personal"]["endpoint"],
        producer.TARGETS["ivan_personal"]["host"],
    )
    assert stream.ingest_record_offset.call_count == 2
    assert [call[0] for call in stream.method_calls] == [
        "ingest_record_offset", "ingest_record_offset", "flush", "close"
    ]
    assert report["durability_confirmed"] is True


@pytest.mark.parametrize("failure", ["ingest_record_offset", "flush"])
def test_zerobus_failure_is_not_reported_as_durable(monkeypatch, failure):
    stream = Mock()
    getattr(stream, failure).side_effect = RuntimeError("ingest failed")
    stream.close.side_effect = RuntimeError("close also failed")
    sdk = Mock()
    sdk.create_stream.return_value = stream
    monkeypatch.setattr(producer, "ZerobusSdk", Mock(return_value=sdk))
    with pytest.raises(RuntimeError, match="ingest failed"):
        producer.send([record()], "ivan_personal", "demo_bronze", ("id", "secret"))
    stream.close.assert_called_once()


def test_zerobus_persists_before_sending_and_replays_exact_records(tmp_path, monkeypatch):
    path = tmp_path / "capture.json"
    item = record()
    capture = Mock(return_value=[item])
    monkeypatch.setattr(producer, "capture", capture)
    monkeypatch.setattr(producer, "credentials", Mock(return_value=("id", "secret")))
    waiter = Mock(return_value={"queryable_confirmed": True})
    monkeypatch.setattr(producer, "wait_for_inbox", waiter)

    def send(records, target, schema, auth):
        saved = json.loads(path.read_text())
        assert saved["records"] == [item]
        assert records == [item, item]
        return {"durability_confirmed": True}

    sender = Mock(side_effect=send)
    monkeypatch.setattr(producer, "send", sender)
    producer.main(["--count", "1", "--duplicates", "1", "--manifest", str(path)])
    producer.main(["--replay", "--duplicates", "1", "--manifest", str(path)])
    capture.assert_called_once()
    assert sender.call_count == 2
    assert waiter.call_count == 2
    assert waiter.call_args.args[0] == [item]
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError, match="target"):
        producer.main([
            "--target", "azure_trial_2", "--replay", "--manifest", str(path)
        ])
    with pytest.raises(ValueError, match="table"):
        producer.main([
            "--bronze-schema", "another", "--replay", "--manifest", str(path)
        ])
    assert sender.call_count == 2


def test_zerobus_failed_send_leaves_replay_manifest(tmp_path, monkeypatch):
    path = tmp_path / "failed.json"
    item = record()
    monkeypatch.setattr(producer, "capture", Mock(return_value=[item]))
    monkeypatch.setattr(producer, "credentials", Mock(return_value=("id", "secret")))
    monkeypatch.setattr(producer, "send", Mock(side_effect=RuntimeError("unavailable")))
    with pytest.raises(RuntimeError, match="unavailable"):
        producer.main(["--manifest", str(path)])
    assert json.loads(path.read_text())["records"] == [item]


def test_zerobus_rejects_tampered_manifest():
    item = record()
    item["event_key"] = "different-id"
    with pytest.raises(ValueError, match="meta.id"):
        producer.validate_records([item])


def test_zerobus_adapter_keeps_silver_contract_and_dq(spark):
    item = record()
    inbox = spark.createDataFrame([item])
    bronze = decode_zerobus_events(inbox)
    row = compute_dq_flags(parse_bronze_to_silver(bronze)).first()
    assert row.event_id == valid_event()["id"]
    assert row.meta_id == item["event_key"]
    assert row._source == "zerobus_wikimedia_recentchange"
    assert row.kafka_offset is None
    assert row.kafka_topic is None
    assert row.kafka_partition is None
    assert row.kafka_enqueued_at is None
    assert row._dq_passed is True


def test_zerobus_setup_creates_external_inbox_and_grants():
    params = {
        "catalog": "workspace", "bronze_schema": "demo_bronze",
        "silver_schema": "demo_silver", "gold_schema": "demo_gold",
        "zerobus_service_principal": "example-id",
    }
    widgets = SimpleNamespace(
        text=lambda key, default: params.setdefault(key, default),
        get=lambda key: params[key],
    )
    sql = Mock()
    context = {"dbutils": SimpleNamespace(widgets=widgets), "spark": SimpleNamespace(sql=sql)}
    for name in ("config.py", "setup.py", "setup_inbox.py"):
        exec(compile((ROOT / "notebooks" / name).read_text(), name, "exec"), context)
    statements = [call.args[0] for call in sql.call_args_list]
    assert "zerobus_wikimedia_inbox" in statements[3]
    assert "USING DELTA" in statements[3]
    assert "delta.appendOnly" in statements[3]
    assert statements[-1].endswith("TO `example-id`")
    assert "SELECT, MODIFY" in statements[-1]


def test_silver_pipeline_reads_inbox_directly_without_persisting_another_bronze(spark, monkeypatch):
    import pyspark

    decorator = lambda **kwargs: lambda function: function
    dp = SimpleNamespace(
        temporary_view=decorator, table=decorator, expect_all_or_drop=lambda rules: lambda f: f,
    )
    monkeypatch.setattr(pyspark, "pipelines", dp, raising=False)
    configuration = {
        "wikimedia.catalog": "workspace",
        "wikimedia.bronze_schema": "demo_bronze",
    }
    read = Mock(return_value=spark.createDataFrame([record()]))
    pipeline_spark = SimpleNamespace(
        conf=SimpleNamespace(get=lambda name, default=None: configuration.get(name, default)),
        readStream=SimpleNamespace(table=read),
    )
    path = ROOT / "pipelines/streaming_medallion/silver_wikipedia_edits.py"
    context = {"spark": pipeline_spark, "__file__": str(path)}
    exec(compile(path.read_text(), str(path), "exec"), context)
    silver = context["silver_wikipedia_edits_staging"]()
    read.assert_called_once_with("workspace.demo_bronze.zerobus_wikimedia_inbox")
    row = silver.first()
    assert row._source == "zerobus_wikimedia_recentchange"
    assert row.meta_id == record()["event_key"]
    assert row._dq_passed is True


def test_prod_producer_defaults_match_bundle_raw_event_destination():
    import yaml

    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    variables = bundle["targets"]["azure_trial_2"]["variables"]
    expected = ".".join(variables[name] for name in ("catalog", "bronze_schema", "raw_events_table"))
    assert producer.table_name("azure_trial_2") == expected
