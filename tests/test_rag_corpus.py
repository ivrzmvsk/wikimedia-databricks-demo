from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from wikimedia_rag.corpus import (cached_document, chunk_diff, chunk_document, corpus_id, generation,
                                  interval, parse_summary, rank_events, summary_url)
from wikimedia_rag.enrichment import BudgetExhausted, PageError, SummaryClient
from wikimedia_rag.storage import candidates


START, END = "2026-10-15T00:00:00Z", "2026-10-15T01:00:00Z"


def payload(title="Earth", pageid=9228, extract="Earth is the third planet from the Sun."):
    return dict(type="standard", title=title, pageid=pageid, extract=extract,
                description="Planet", revision="123", content_urls={"desktop": {"page":
                    "https://en.wikipedia.org/wiki/Earth"}})


@pytest.mark.parametrize("start,end", [("2026-10-15T00:00:00", END), (END, START),
                                       ("2026-10-15T00:00:00+02:00", END), (START, START)])
def test_utc_interval_rejects_invalid(start, end):
    with pytest.raises(ValueError):
        interval(start, end)


def test_selection_boundaries_filters_and_ties(spark):
    base = dict(wiki="enwiki", namespace=0, event_type="edit", title="A", event_time=START)
    events = [base, dict(base, title="Z"), dict(base, title="Z"), dict(base, title="B"),
              dict(base, title="at end", event_time=END), dict(base, wiki="dewiki"),
              dict(base, namespace=1), dict(base, event_type="log"), dict(base, title=" "),
              dict(base, event_time="2026-10-14T23:59:59Z")]
    expected = [dict(wiki="enwiki", title="Z", event_count=2),
                dict(wiki="enwiki", title="A", event_count=1), dict(wiki="enwiki", title="B", event_count=1)]
    assert rank_events(events, START, END) == expected
    rows = [dict(e, event_time=datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))) for e in events]
    spark.createDataFrame(rows).createOrReplaceTempView("rag_events")
    proxy = SimpleNamespace(conf=spark.conf, table=lambda _: spark.table("rag_events"))
    assert [r.asDict() for r in candidates(proxy, "workspace", "silver", START, END, 3).collect()] == expected


def test_url_encoding():
    assert summary_url("A/B & café?").endswith("A%2FB%20%26%20caf%C3%A9%3F")


def test_canonical_identity_and_hash_exclude_technical_timestamps():
    a = parse_summary(payload(), START)
    b = parse_summary(payload(), END)
    assert a["document_id"] == b["document_id"] == "enwiki:9228"
    assert a["content_hash"] == b["content_hash"]
    assert a["content_hash"] != parse_summary(payload(extract="Changed"), START)["content_hash"]
    assert parse_summary(payload(title="Redirect target"), START)["document_id"] == a["document_id"]


@pytest.mark.parametrize("change", [dict(type="disambiguation"), dict(type="missing"), dict(extract=""),
                                    dict(pageid=None), dict(pageid=True), dict(title=""), dict(description=[]),
                                    dict(content_urls={"desktop": {"page": "https://evil.test/wiki/Earth"}})])
def test_unusable_response(change):
    with pytest.raises(ValueError):
        parse_summary(dict(payload(), **change), START)


def test_cache_policy_and_chunks_replacement():
    doc = parse_summary(payload(extract="x" * 500), START)
    assert cached_document(doc, END, 2, "expired") == doc
    assert cached_document(doc, END, 0.5, "expired") is None
    assert cached_document(doc, END, 0.5, "missing") == doc
    assert cached_document(doc, END, 2, "force") is None
    corpus = corpus_id(START, END, 1)
    old = chunk_document(doc, corpus, START, END, size=100)
    new = chunk_document(parse_summary(payload(extract="y"), START), corpus, START, END, size=100)
    assert old[0]["chunk_id"] == new[0]["chunk_id"]
    changed, removed = chunk_diff(old, new)
    assert changed == new and removed == sorted(c["chunk_id"] for c in old[1:])
    assert chunk_diff(old, old) == ([], [])
    assert generation(old) == generation(list(reversed(old)))
    assert generation(old) != generation(new)
    assert all(START not in c["text"] for c in old)


def response(status, data=None, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {}, json=lambda: data)


def http(responses, **kwargs):
    session = Mock()
    session.get.side_effect = responses
    sleeps = []
    return SummaryClient("https://example.org/contact", session=session, sleep=sleeps.append,
                         clock=lambda: 0, **kwargs), session, sleeps


def test_retry_after_and_global_attempt_budget():
    client, session, sleeps = http([response(429, headers={"Retry-After": "5"}), response(200, payload())], max_attempts=2)
    assert client.fetch("Earth", START)["page_id"] == 9228
    assert sleeps[-1] >= 5
    with pytest.raises(BudgetExhausted):
        client.fetch("Mars", START)
    assert session.get.call_count == 2
    assert session.get.call_args.kwargs["allow_redirects"] is False


@pytest.mark.parametrize("responses", [[response(404)], [response(302)], [response(200, [])],
                                        [response(429, headers={"Retry-After": "999"})],
                                        [response(503)] * 3, [requests.Timeout()] * 3])
def test_http_failures_are_reportable_and_bounded(responses):
    client, session, _ = http(responses)
    with pytest.raises(PageError):
        client.fetch("Earth", START)
    assert session.get.call_count <= 3


def test_http_redirect_consumes_budget_and_uses_canonical_id():
    client, session, _ = http([response(301, headers={"Location": "/api/rest_v1/page/summary/Earth"}),
                               response(200, payload())], max_attempts=2)
    assert client.fetch("Redirect", START)["document_id"] == "enwiki:9228"
    assert client.attempts == 2
    assert session.get.call_args.args[0] == summary_url("Earth")


def test_http_redirect_cannot_leave_wikimedia():
    client, session, _ = http([response(301, headers={"Location": "https://evil.test/"})])
    with pytest.raises(PageError, match="Unsafe"):
        client.fetch("Earth", START)
    assert session.get.call_count == 1


@pytest.mark.parametrize("case", ["success", "redirects", "mass_failure", "cache"])
def test_preparation_publication_and_canonical_dedup(monkeypatch, case):
    from wikimedia_rag import storage
    store, client = Mock(), Mock()
    store.cache_many.return_value = {"Earth": parse_summary(payload(), START)} if case == "cache" else {}
    rows = [SimpleNamespace(title=t, asDict=lambda t=t: dict(wiki="enwiki", title=t, event_count=1))
            for t in ["Earth", "Redirect", "Mars"]]
    monkeypatch.setattr(storage, "Store", lambda *args: store)
    monkeypatch.setattr(storage, "candidates", lambda *args: SimpleNamespace(collect=lambda: rows))
    monkeypatch.setattr(storage, "SummaryClient", lambda *args, **kwargs: client)
    client.attempts = 0 if case == "cache" else 3
    client.fetch.side_effect = ([PageError("HTTP 503")] * 3 if case == "mass_failure" else
                               [parse_summary(payload(), START), parse_summary(payload(), START),
                                parse_summary(payload(title="Mars", pageid=2), START)])
    limit = 2 if case == "redirects" else 1
    if case == "mass_failure":
        with pytest.raises(RuntimeError, match="not published"):
            storage.prepare(None, catalog="workspace", silver_schema="silver", ai_schema="ai", start=START,
                            end=END, article_limit=limit, max_attempts=3, contact="contact")
        store.reconcile_chunks.assert_not_called()
        assert store.manifest.call_args.args[1] == "FAILED"
        return
    report = storage.prepare(None, catalog="workspace", silver_schema="silver", ai_schema="ai", start=START,
                             end=END, article_limit=limit, max_attempts=3, contact="contact", refresh="missing")
    assert len(report["documents"]) == limit
    assert store.manifest.call_args.args[1] == "PREPARED"
    if case == "redirects":
        assert report["pages"][1]["status"] == "duplicate_redirect"
        assert len({d["document_id"] for d in report["documents"]}) == 2
    if case == "cache":
        client.fetch.assert_not_called()
        assert report["http_attempts"] == 0


def test_curated_corpus_identity_is_stable_and_distinct():
    from wikimedia_rag.corpus import normalize_titles
    assert corpus_id(START, END, 2, ["Mars", "Earth"]) == corpus_id(START, END, 2, ["Earth", "Mars"])
    assert corpus_id(START, END, 2, ["Earth", "Mars"]) != corpus_id(START, END, 2)
    assert corpus_id(START, END, 1, []) == corpus_id(START, END, 1)
    for titles in (["Earth", "Earth"], ["Earth"], ["Earth", " "], "Earth,Mars", ["Earth", 42]):
        with pytest.raises(ValueError):
            normalize_titles(titles, 2)


def test_curated_selection_excludes_higher_frequency_unselected_titles(spark):
    from wikimedia_rag.corpus import utc
    events = [dict(wiki="enwiki", namespace=0, event_type="edit", title=title, event_time=utc(START))
              for title in ["Unselected"] * 5 + ["Earth", "Mars"]]
    spark.createDataFrame(events).createOrReplaceTempView("rag_curated_events")
    proxy = SimpleNamespace(conf=spark.conf, table=lambda _: spark.table("rag_curated_events"))
    selected = candidates(proxy, "workspace", "silver", START, END, 10, ["Mars", "Earth"]).collect()
    assert [r.title for r in selected] == ["Earth", "Mars"]


def test_missing_curated_titles_fail_before_http_or_publication(monkeypatch):
    from wikimedia_rag import storage
    store, client = Mock(), Mock()
    monkeypatch.setattr(storage, "Store", lambda *args: store)
    monkeypatch.setattr(storage, "SummaryClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(storage, "candidates", lambda *args: SimpleNamespace(collect=lambda: []))
    with pytest.raises(RuntimeError, match="no eligible Silver"):
        storage.prepare(None, catalog="workspace", silver_schema="silver", ai_schema="ai", start=START,
                        end=END, article_limit=1, max_attempts=1, contact="contact", titles=["Earth"])
    client.fetch.assert_not_called()
    store.reconcile_chunks.assert_not_called()


def test_corpus_limit_accepts_40_and_rejects_more():
    assert corpus_id(START, END, 40)
    with pytest.raises(ValueError, match="1..40"):
        corpus_id(START, END, 41)


def test_bulk_cache_matches_aliases_to_documents(spark):
    from wikimedia_rag.storage import Store, CONTENT_SCHEMA, ALIAS_SCHEMA
    doc = parse_summary(payload(), START)
    spark.createDataFrame([doc], CONTENT_SCHEMA).createOrReplaceTempView("rag_cache_content")
    spark.createDataFrame([("enwiki", "Earth", doc["document_id"]),
                           ("enwiki", "Terra", doc["document_id"]),
                           ("enwiki", "Missing", "missing")], ALIAS_SCHEMA).createOrReplaceTempView("rag_cache_aliases")
    views = {"cache.article_content": "rag_cache_content", "cache.article_aliases": "rag_cache_aliases"}
    store = Store.__new__(Store)
    store.prefix = "cache"
    store.spark = SimpleNamespace(table=lambda name: spark.table(views[name]))
    assert store.cache_many(["Terra", "Missing"]) == {"Terra": doc}
