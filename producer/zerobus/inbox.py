"""Wait for durable Zerobus events to become queryable in Delta."""

from contextlib import suppress
import json
import logging
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.sql import StatementParameterListItem, StatementState


LOGGER = logging.getLogger(__name__)


def wait_for_inbox(records, table, target, *, warehouse_id=None, timeout=180, poll_interval=2):
    keys = sorted({record["event_key"] for record in records})
    if not keys or timeout <= 0 or poll_interval <= 0:
        raise ValueError("Inbox keys, timeout and poll interval must be nonempty/positive")
    deadline = time.monotonic() + timeout
    warehouse_id = warehouse_id or os.getenv("BUNDLE_VAR_warehouse_id") or target["warehouse_id"]
    client = WorkspaceClient(config=Config(
        profile=target["profile"],
        http_timeout_seconds=min(5, timeout),
        retry_timeout_seconds=1,
    ))
    if client.config.host.rstrip("/") != target["host"].rstrip("/"):
        raise ValueError("SQL profile must resolve to the selected Zerobus workspace")
    statement_id = None
    visible = 0
    try:
        while time.monotonic() < deadline:
            if statement_id is None:
                response = client.statement_execution.execute_statement(
                    statement=f"""SELECT COUNT(DISTINCT event_key) FROM {table}
                        WHERE array_contains(from_json(:event_keys, 'ARRAY<STRING>'), event_key)""",
                    warehouse_id=warehouse_id,
                    parameters=[StatementParameterListItem(
                        name="event_keys", value=json.dumps(keys), type="STRING"
                    )],
                    wait_timeout="0s",
                )
                statement_id = response.statement_id
            else:
                response = client.statement_execution.get_statement(statement_id)
            if time.monotonic() >= deadline:
                break
            state = response.status.state
            if state == StatementState.SUCCEEDED:
                statement_id = None
                visible = int(response.result.data_array[0][0])
                if visible == len(keys):
                    return {"queryable_confirmed": True, "visible_unique_events": visible}
                LOGGER.info("Inbox visibility: %s/%s unique events", visible, len(keys))
            elif state not in (StatementState.PENDING, StatementState.RUNNING):
                raise RuntimeError(f"Inbox visibility query failed: {state}")
            time.sleep(max(0, min(poll_interval, deadline - time.monotonic())))
        raise TimeoutError(f"Inbox visibility deadline exceeded: {visible}/{len(keys)} unique events")
    finally:
        if statement_id:
            with suppress(Exception):
                client.statement_execution.cancel_execution(statement_id)
