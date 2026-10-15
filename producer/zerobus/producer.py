"""Capture Wikimedia edit events to a replay manifest and ingest with Zerobus."""

import argparse
from contextlib import suppress
from datetime import datetime, timezone
import json
import logging
import multiprocessing
import os
from pathlib import Path
import re
import time

from dotenv import dotenv_values
from requests_sse import EventSource
from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

if __package__:
    from .inbox import wait_for_inbox
else:
    from inbox import wait_for_inbox


TARGETS = json.loads(Path(__file__).with_name("targets.json").read_text())
SOURCE = "https://stream.wikimedia.org/v2/stream/recentchange"
LOGGER = logging.getLogger(__name__)


def table_name(target, bronze_schema=None):
    # Catalog/workspace are pinned together. Defaults match the bundle target.
    config = TARGETS[target]
    bronze_schema = bronze_schema or config.get("bronze_schema", "wikimediademo_bronze")
    raw_events_table = config.get("raw_events_table", "zerobus_wikimedia_inbox")
    for value in (bronze_schema, raw_events_table):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Bronze schema and raw table must be simple SQL identifiers")
    return f"{config['catalog']}.{bronze_schema}.{raw_events_table}"


def credentials(env_file):
    # An explicit file is isolated from inherited credentials for another target.
    values = dotenv_values(env_file, interpolate=False) if env_file else os.environ
    keys = ("ZEROBUS_CLIENT_ID", "ZEROBUS_CLIENT_SECRET")
    if not all(values.get(key) for key in keys):
        raise ValueError("Set ZEROBUS_CLIENT_ID and ZEROBUS_CLIENT_SECRET")
    return tuple(values[key] for key in keys)


def normalize(change):
    if not isinstance(change, dict) or not isinstance(change.get("meta"), dict):
        raise ValueError("Expected a Wikimedia event with metadata")
    if change["meta"].get("domain") == "canary":
        return None
    if change.get("type") != "edit":
        return None
    event_key = change["meta"].get("id")
    if not isinstance(event_key, str) or not event_key.strip():
        raise ValueError("Wikimedia event must have a nonempty meta.id")
    payload = dict(change)
    payload["_producer"] = {
        "source": "wikimedia-recentchange",
        "producer_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    return {
        "event_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        "event_key": event_key,
        "produced_at": time.time_ns() // 1000,
    }


def source_error():
    raise ConnectionError("Wikimedia SSE interrupted; retry capture explicitly")


def _read_capture(count, timeout, url):
    records = {}
    deadline = time.monotonic() + timeout
    with EventSource(
        url,
        timeout=min(timeout, 10),
        max_connect_retry=0,
        on_error=source_error,
        headers={"User-Agent": "wikimedia-databricks-demo/1.0"},
    ) as source:
        for event in source:
            if time.monotonic() >= deadline:
                raise TimeoutError("Wikimedia capture exceeded its deadline")
            if event.type != "message":
                continue
            try:
                record = normalize(json.loads(event.data))
            except (ValueError, TypeError):
                LOGGER.warning("Skipping malformed Wikimedia event")
                continue
            if record:
                records.setdefault(record["event_key"], record)
            if len(records) == count:
                return list(records.values())
    raise RuntimeError("Wikimedia stream ended before the requested count")


def _capture_worker(connection, count, timeout, url):
    try:
        connection.send((_read_capture(count, timeout, url), None))
    except Exception as error:
        connection.send((None, f"{type(error).__name__}: {error}"))
    finally:
        connection.close()


def capture(count, timeout):
    """Bound the entire SSE read, including connections that only send comments."""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    worker = context.Process(
        target=_capture_worker, args=(sender, count, timeout, SOURCE), daemon=True
    )
    deadline = time.monotonic() + timeout
    worker.start()
    sender.close()
    try:
        if not receiver.poll(max(0, deadline - time.monotonic())):
            raise TimeoutError("Wikimedia capture exceeded its deadline")
        records, error = receiver.recv()
        if error:
            raise RuntimeError(error)
        return records
    finally:
        receiver.close()
        if worker.is_alive():
            worker.terminate()
        worker.join(timeout=1)
        if worker.is_alive():
            worker.kill()
            worker.join(timeout=1)
        if not worker.is_alive():
            worker.close()


def validate_records(records):
    if not isinstance(records, list) or not records:
        raise ValueError("Manifest records must be a nonempty list")
    keys = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != {
            "event_json", "event_key", "produced_at"
        }:
            raise ValueError("Manifest record does not match the Zerobus inbox schema")
        event_key = record["event_key"]
        if not isinstance(event_key, str) or not event_key.strip() or event_key in keys:
            raise ValueError("Manifest event keys must be unique and nonempty")
        if type(record["produced_at"]) is not int or record["produced_at"] <= 0:
            raise ValueError("Manifest produced_at must be positive microseconds")
        payload = json.loads(record["event_json"])
        if not isinstance(payload, dict) or not isinstance(payload.get("meta"), dict):
            raise ValueError("Manifest payload must be a Wikimedia object")
        if payload["meta"].get("id") != event_key:
            raise ValueError("Manifest key must match payload meta.id")
        if payload.get("type") != "edit":
            raise ValueError("Manifest payload must have type='edit'; capture a new manifest")
        keys.add(event_key)


def send(records, target, bronze_schema, auth):
    config = TARGETS[target]
    table = table_name(target, bronze_schema)
    sdk = ZerobusSdk(config["endpoint"], config["host"])
    options = StreamConfigurationOptions(
        flush_timeout_ms=60_000,
        server_lack_of_ack_timeout_ms=30_000,
        recovery_retries=2,
        recovery_timeout_ms=15_000,
        max_inflight_records=1000,
    )
    stream = sdk.create_stream(*auth, TableProperties(table), options)
    try:
        for record in records:
            stream.ingest_record_offset(record)
        stream.flush()
    except Exception:
        with suppress(Exception):
            stream.close()
        raise
    else:
        stream.close()
    return {"submitted": len(records), "table": table, "durability_confirmed": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=tuple(TARGETS), default="ivan_personal")
    parser.add_argument("--bronze-schema", help="Override the target Bronze schema")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--visibility-timeout", type=int, default=180)
    parser.add_argument("--warehouse-id", help="SQL warehouse used to confirm inbox visibility")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--duplicates", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.count < 1 or args.timeout < 1 or args.visibility_timeout < 1 or args.duplicates < 0:
        parser.error("count/timeouts must be positive; duplicates must be nonnegative")
    table_name(args.target, args.bronze_schema)
    # Fail before capturing if auth is missing. Dry runs never need credentials.
    auth = credentials(args.env_file) if not args.dry_run else None
    if args.replay:
        manifest = json.loads(args.manifest.read_text())
        if manifest.get("target") != args.target:
            raise ValueError("Replay target must match the captured target")
        if manifest.get("table") != table_name(args.target, args.bronze_schema):
            raise ValueError("Replay table must match the captured table")
        records = manifest["records"]
        validate_records(records)
    else:
        if args.manifest.exists():
            parser.error("manifest already exists; use --replay or choose a new path")
        if args.duplicates > args.count:
            parser.error("duplicates must not exceed count")
        records = capture(args.count, args.timeout)
        validate_records(records)
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        # Persist before ingestion; exclusive creation prevents overwriting a replay.
        descriptor = os.open(args.manifest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump({
                "target": args.target,
                "table": table_name(args.target, args.bronze_schema),
                "source": SOURCE,
                "records": records,
            }, output, ensure_ascii=False)
            output.flush()
            os.fsync(output.fileno())
    if args.duplicates > len(records):
        parser.error("duplicates must not exceed captured count")
    report = {"manifest": str(args.manifest), "unique_events": len(records)}
    if not args.dry_run:
        report.update(send(
            records + records[:args.duplicates], args.target, args.bronze_schema, auth
        ))
        report.update(wait_for_inbox(
            records, table_name(args.target, args.bronze_schema), TARGETS[args.target],
            warehouse_id=args.warehouse_id, timeout=args.visibility_timeout,
        ))
    print(json.dumps(report))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
