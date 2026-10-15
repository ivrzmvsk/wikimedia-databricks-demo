from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
from types import SimpleNamespace
import threading
import time
from unittest.mock import Mock

import pytest

from databricks.sdk.service.sql import StatementState
from producer.zerobus import inbox, producer
from tests.stream_test_helpers import valid_event


def response(state, count=None):
    return SimpleNamespace(
        statement_id="statement-1", status=SimpleNamespace(state=state),
        result=SimpleNamespace(data_array=[[str(count)]]) if count is not None else None,
    )


@pytest.fixture
def visibility(monkeypatch):
    clock = SimpleNamespace(value=0)

    def sleep(seconds):
        clock.value += seconds

    monkeypatch.setattr(inbox, "time", SimpleNamespace(
        monotonic=lambda: clock.value, sleep=sleep,
    ))
    monkeypatch.setattr(inbox, "Config", lambda **kwargs: SimpleNamespace(**kwargs))
    target = producer.TARGETS["ivan_personal"]
    client = Mock()
    client.config.host = target["host"]
    factory = Mock(return_value=client)
    monkeypatch.setattr(inbox, "WorkspaceClient", factory)
    records = [{"event_key": "key-1"}, {"event_key": "key-'2"}, {"event_key": "key-1"}]
    wait = partial(
        inbox.wait_for_inbox, records, "workspace.bronze.zerobus_wikimedia_inbox", target,
        timeout=3, poll_interval=1,
    )
    return SimpleNamespace(wait=wait, client=client, api=client.statement_execution,
                           factory=factory, clock=clock)


def test_inbox_waits_for_every_unique_manifest_key(visibility):
    v = visibility
    v.api.execute_statement.side_effect = [
        response(StatementState.SUCCEEDED, n) for n in (0, 1, 2)
    ]
    assert v.wait() == {"queryable_confirmed": True, "visible_unique_events": 2}
    assert v.api.execute_statement.call_count == 3
    query = v.api.execute_statement.call_args.kwargs
    assert query["wait_timeout"] == "0s"
    assert "COUNT(DISTINCT event_key)" in query["statement"]
    assert "array_contains(from_json(:event_keys" in query["statement"]
    assert "key-'2" not in query["statement"]
    assert set(json.loads(query["parameters"][0].value)) == {"key-1", "key-'2"}
    v.api.cancel_execution.assert_not_called()


def test_inbox_polls_pending_statement_before_issuing_another(visibility):
    v = visibility
    v.api.execute_statement.return_value = response(StatementState.PENDING)
    v.api.get_statement.side_effect = [
        response(StatementState.RUNNING), response(StatementState.SUCCEEDED, 2)
    ]
    assert v.wait()["queryable_confirmed"] is True
    v.api.execute_statement.assert_called_once()
    assert v.api.get_statement.call_count == 2


def test_inbox_timeout_cancels_unfinished_query(visibility):
    v = visibility
    v.api.execute_statement.return_value = response(StatementState.PENDING)
    v.api.get_statement.return_value = response(StatementState.RUNNING)
    with pytest.raises(TimeoutError, match="0/2"):
        v.wait()
    assert v.clock.value == 3
    v.api.cancel_execution.assert_called_once_with("statement-1")


def test_inbox_partial_visibility_is_not_success(visibility):
    v = visibility
    v.api.execute_statement.return_value = response(StatementState.SUCCEEDED, 1)
    with pytest.raises(TimeoutError, match="1/2"):
        v.wait()
    assert v.api.execute_statement.call_count == 3


def test_inbox_query_failure_stops_processing(visibility):
    visibility.api.execute_statement.return_value = response(StatementState.FAILED)
    with pytest.raises(RuntimeError, match="query failed"):
        visibility.wait()


def test_inbox_rejects_profile_for_another_workspace(visibility):
    visibility.client.config.host = "https://another-workspace.cloud.databricks.com"
    with pytest.raises(ValueError, match="selected Zerobus workspace"):
        visibility.wait()
    visibility.api.execute_statement.assert_not_called()


def test_inbox_rejects_success_returned_after_deadline(visibility):
    def delayed_query(**kwargs):
        visibility.clock.value = 4
        return response(StatementState.SUCCEEDED, 2)

    visibility.api.execute_statement.side_effect = delayed_query
    with pytest.raises(TimeoutError):
        visibility.wait()


def test_visibility_failure_keeps_manifest_and_does_not_report_success(tmp_path, monkeypatch, capsys):
    item = producer.normalize(valid_event())
    monkeypatch.setattr(producer, "capture", Mock(return_value=[item]))
    monkeypatch.setattr(producer, "credentials", Mock(return_value=("id", "secret")))
    monkeypatch.setattr(producer, "send", Mock(return_value={"durability_confirmed": True}))
    monkeypatch.setattr(producer, "wait_for_inbox", Mock(side_effect=TimeoutError("not visible")))
    path = tmp_path / "capture.json"
    with pytest.raises(TimeoutError, match="not visible"):
        producer.main(["--manifest", str(path)])
    assert json.loads(path.read_text())["records"] == [item]
    assert capsys.readouterr().out == ""


@pytest.fixture
def sse_server(monkeypatch):
    connected = threading.Event()
    stopped = threading.Event()
    sent = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            connected.set()
            payload = (
                b": heartbeat " + b"x" * 1024 + b"\n\n"
                if self.path == "/heartbeat"
                else ("data: " + json.dumps(valid_event()) + "\n\n").encode()
            )
            try:
                while not stopped.is_set():
                    self.wfile.write(payload)
                    self.wfile.flush()
                    sent.append(1)
                    if self.path != "/heartbeat":
                        return
                    stopped.wait(0.01)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield SimpleNamespace(url=url, connected=connected, sent=sent)
    finally:
        stopped.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_capture_deadline_interrupts_real_heartbeat_only_sse(sse_server, monkeypatch):
    monkeypatch.setattr(producer, "SOURCE", sse_server.url + "/heartbeat")
    previous_children = {p.pid for p in multiprocessing.active_children()}
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="deadline"):
        producer.capture(1, 2)
    assert time.monotonic() - started < 4
    assert sse_server.connected.is_set()
    assert sse_server.sent
    assert {p.pid for p in multiprocessing.active_children()} == previous_children


def test_capture_worker_returns_events_and_closes(sse_server, monkeypatch):
    monkeypatch.setattr(producer, "SOURCE", sse_server.url + "/event")
    previous_children = {p.pid for p in multiprocessing.active_children()}
    records = producer.capture(1, 5)
    assert len(records) == 1
    assert records[0]["event_key"] == valid_event()["meta"]["id"]
    assert {p.pid for p in multiprocessing.active_children()} == previous_children
