import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
import yaml

from wikimedia_rag.search import sync_and_wait

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("retrieval", ROOT / "app/wikimedia_assistant/retrieval.py")
retrieval = importlib.util.module_from_spec(spec)
spec.loader.exec_module(retrieval)


def row(**kwargs):
    return dict(chunk_id="a", text="Earth is a planet.", source_url="https://en.wikipedia.org/wiki/Earth",
                title="Earth", fetched_at="2026-10-15T00:00:00Z", revision="123", corpus_id="c",
                generation="g", document_type="wikipedia_summary", start_utc="s", end_utc="e", **kwargs)


def test_managed_retrieval_filters_and_sources():
    client = Mock()
    names = retrieval.COLUMNS
    r = row()
    client.vector_search_indexes.query_index.return_value.as_dict.return_value = {
        "manifest": {"columns": [{"name": c} for c in names]},
        "result": {"data_array": [[r[c] for c in names], [dict(r, generation="old")[c] for c in names]]}}
    found = retrieval.retrieve(client, "index", "Earth?", "c", "g")
    assert found == [r]
    call = client.vector_search_indexes.query_index.call_args.kwargs
    assert json.loads(call["filters_json"]) == dict(corpus_id="c", generation="g")
    assert call["num_results"] == 4
    client.api_client.do.return_value = {"choices": [{"message": {"content": json.dumps(
        dict(supported=True, answer="Earth is a planet.", source_ids=[1]))}}]}
    result = retrieval.answer(client, "model", "Earth?", found)
    assert result["sources"][0]["url"] == r["source_url"]
    assert "text" not in result["sources"][0]
    assert sum(len(s["text"]) for s in retrieval.context([r] * 8, max_chars=20)) == 20


def test_empty_context_skips_llm_and_services_fail_closed():
    client = Mock()
    assert retrieval.answer(client, "model", "question", [])["supported"] is False
    client.api_client.do.assert_not_called()
    client.vector_search_indexes.query_index.side_effect = RuntimeError("permission denied")
    with pytest.raises(retrieval.ServiceError):
        retrieval.retrieve(client, "index", "question", "c", "g")
    client.api_client.do.side_effect = RuntimeError("model missing")
    with pytest.raises(retrieval.ServiceError):
        retrieval.answer(client, "model", "question", [row()])


@pytest.mark.parametrize("parsed", [dict(supported=False), dict(supported=True, answer="x", source_ids=[9]),
                                    dict(supported=True, answer="x", source_ids=[]),
                                    dict(supported=True, answer="x", source_ids=[True])])
def test_unsupported_or_invalid_citations_refused(parsed):
    client = Mock()
    client.api_client.do.return_value = {"choices": [{"message": {"content": json.dumps(parsed)}}]}
    assert retrieval.answer(client, "model", "question", [row()])["supported"] is False


def sync_client():
    client = Mock()
    spec = NS(source_table="c.s.chunks", pipeline_type=NS(value="TRIGGERED"), pipeline_id="p",
              embedding_source_columns=[NS(name="text", embedding_model_endpoint_name="embed")])
    client.vector_search_indexes.get_index.return_value = NS(delta_sync_index_spec=spec, primary_key="chunk_id",
                                                            status=NS(ready=True, indexed_row_count=1))
    return client


def update(uid, state):
    return NS(update_id=uid, state=NS(value=state))


def test_sync_does_not_accept_stale_ready():
    client = sync_client()
    old, new = update("old", "COMPLETED"), update("new", "COMPLETED")
    client.pipelines.list_updates.side_effect = [NS(updates=[old]), NS(updates=[old]), NS(updates=[new, old])]
    client.pipelines.get_update.return_value = NS(update=new)
    ticks = iter([0, 0, 1, 1, 2])
    result = sync_and_wait(client, "index", "c.s.chunks", "embed", sleep=lambda _: None, clock=lambda: next(ticks))
    assert result["update_id"] == "new"
    assert client.pipelines.list_updates.call_count == 3


@pytest.mark.parametrize("state", ["FAILED", "CANCELED"])
def test_sync_failure(state):
    client = sync_client()
    client.pipelines.list_updates.side_effect = [NS(updates=[]), NS(updates=[update("new", state)])]
    client.pipelines.get_update.return_value = NS(update=update("new", state))
    with pytest.raises(RuntimeError):
        sync_and_wait(client, "index", "c.s.chunks", "embed")


def test_sync_timeout_and_model_migration_guard():
    client = sync_client()
    with pytest.raises(RuntimeError, match="configuration differs"):
        sync_and_wait(client, "index", "c.s.chunks", "other-model")
    client.vector_search_indexes.sync_index.assert_not_called()
    client.pipelines.list_updates.return_value = NS(updates=[])
    ticks = iter([0, 2])
    with pytest.raises(TimeoutError):
        sync_and_wait(client, "index", "c.s.chunks", "embed", timeout=1, clock=lambda: next(ticks))


def test_separate_bundles_paths_defaults_and_privileges():
    main = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "engine" not in main["bundle"]
    prep = yaml.safe_load((ROOT / "rag/preparation/databricks.yml").read_text())
    job = prep["resources"]["jobs"]["wikimedia_rag"]
    assert "schedule" not in job
    assert prep["variables"]["article_limit"]["default"] == "1"
    assert prep["variables"]["start"]["default"] == ""
    for task in job["tasks"]:
        assert (ROOT / "rag/preparation" / task["notebook_task"]["notebook_path"]).is_file()
    for path in prep["sync"]["paths"]:
        assert (ROOT / "rag/preparation" / path).is_dir()
    search = yaml.safe_load((ROOT / "rag/serving/search.yml").read_text())
    index = search["resources"]["vector_search_indexes"]["wikimedia_index"]
    assert index["delta_sync_index_spec"]["pipeline_type"] == "TRIGGERED"
    assert index["delta_sync_index_spec"]["embedding_source_columns"][0]["name"] == "text"
    app = yaml.safe_load((ROOT / "rag/application/databricks.yml").read_text())
    resources = app["resources"]["apps"]["wikimedia_assistant"]["resources"]
    assert resources[0]["uc_securable"]["permission"] == "SELECT"
    assert all(r["serving_endpoint"]["permission"] == "CAN_QUERY" for r in resources[1:])
    for config in [prep, app, yaml.safe_load((ROOT / "rag/serving/databricks.yml").read_text())]:
        assert config["bundle"]["engine"] == "direct"
        assert set(config["targets"]) == {"ivan_personal", "azure_trial_2"}


def test_package_refuses_unready_report(tmp_path):
    spec = importlib.util.spec_from_file_location("package", ROOT / "scripts/package_rag_app.py")
    package = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(package)
    report = tmp_path / "report.json"
    report.write_text('{"status":"PREPARED"}')
    with pytest.raises(ValueError, match="sync report"):
        package.package(report, ROOT / "rag/application/assistant")


def test_managed_retry_does_not_mask_original_sync_failure():
    client = sync_client()
    failed = update("requested", "FAILED")
    failed.cause = NS(value="API_CALL")
    retry = update("automatic-retry", "CREATED")
    retry.cause = NS(value="RETRY_ON_FAILURE")
    client.pipelines.list_updates.side_effect = [NS(updates=[]), NS(updates=[retry, failed])]
    client.pipelines.get_update.return_value = NS(update=failed)
    with pytest.raises(RuntimeError, match="requested: FAILED"):
        sync_and_wait(client, "index", "c.s.chunks", "embed")


def test_app_names_fit_cloud_api_limits():
    import re
    config = yaml.safe_load((ROOT / "rag/application/databricks.yml").read_text())
    names = [config["variables"]["app_name"]["default"],
             config["targets"]["azure_trial_2"]["variables"]["app_name"]]
    assert all(2 <= len(n) <= 30 and re.fullmatch(r"[a-z][a-z0-9-]*", n) for n in names)


@pytest.mark.parametrize('question', [
    'Почему редактор внёс конкретную правку в статью Kepler-16?',
    'Why did someone edit this article?',
    'What was the reason for the revision?',
])
def test_summary_cannot_explain_historical_edit_motives(question):
    client = Mock()
    result = retrieval.answer(client, 'model', question, [row()])
    assert result['supported'] is False and not result['sources']
    client.api_client.do.assert_not_called()


def test_refusal_language_follows_question():
    client = Mock()
    assert retrieval.answer(client, "model", "What is missing?", [])["answer"] == retrieval.NO_CONTEXT
    assert "недостаточно" in retrieval.answer(client, "model", "Чего не хватает?", [])["answer"]
    client.api_client.do.assert_not_called()


@pytest.mark.parametrize("url", ["projectdoc:///rag_docs/../secret.md", "projectdoc://remote/rag_docs/a.md",
                                  "projectdoc:///rag_docs/%2e%2e/secret.md", "file:///etc/passwd"])
def test_document_sources_reject_unsafe_paths(url):
    assert retrieval.document_path(url) is None
    assert retrieval.context([dict(row(), source_url=url)]) == []


def test_project_document_citation_reaches_grounded_answer():
    client = Mock()
    doc = dict(row(), source_url="projectdoc:///rag_docs/guide.md", document_type="project_document",
               text="The events job never calls SiteMatrix.", title="Project Guide")
    client.api_client.do.return_value = {"choices": [{"message": {"content": json.dumps(
        dict(supported=True, answer="Events never call SiteMatrix.", source_ids=[1]))}}]}
    result = retrieval.answer(client, "model", "Does the events job call SiteMatrix?", [doc])
    assert result["supported"] and result["sources"][0]["url"] == doc["source_url"]


def test_mixed_sources_still_refuse_specific_wikipedia_edit_motives():
    client = Mock()
    doc = dict(row(), document_type="project_document", source_url="projectdoc:///rag_docs/guide.md")
    result = retrieval.answer(client, "model", "Why did the editor change the Mansa Musa article?", [row(), doc])
    assert not result["supported"] and not result["sources"]
    client.api_client.do.assert_not_called()


def test_mixed_sources_allow_documented_editor_governance_reason():
    client = Mock()
    doc = dict(row(), document_type="project_document", source_url="projectdoc:///rag_docs/guide.md",
               text="Editor names are masked to protect personal data.")
    client.api_client.do.return_value = {"choices": [{"message": {"content": json.dumps(
        dict(supported=True, answer="Editor names are masked to protect personal data.", source_ids=[2]))}}]}
    result = retrieval.answer(client, "model", "Why are editor names masked?", [row(), doc])
    assert result["supported"] and result["sources"][0]["url"] == doc["source_url"]


def test_eval_accepts_either_documented_source_but_rejects_unlisted_source(monkeypatch):
    spec = importlib.util.spec_from_file_location("evaluate", ROOT / "scripts/evaluate_rag.py")
    evaluation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(evaluation)
    config = dict(corpus_id="c", generation="g", documents=[
        dict(canonical_title="A", source_url="projectdoc:///rag_docs/a.md"),
        dict(canonical_title="B", source_url="projectdoc:///rag_docs/b.md")])
    case = dict(question="Data flow?", supported=True, expected_titles=[], expected_any_titles=["A", "B"])
    monkeypatch.setattr(evaluation, "retrieve", lambda *args: [])
    for name, expected in [("b", True), ("unlisted", False)]:
        monkeypatch.setattr(evaluation, "answer", lambda *args: dict(
            supported=True, answer="Documented flow.", sources=[dict(url=f"projectdoc:///rag_docs/{name}.md")]))
        assert evaluation.evaluate(Mock(), config, [case], "index", "model")[0]["passed"] is expected


@pytest.mark.parametrize('question,language', [
    ('What is the architecture?', 'English'),
    ('Как работает архитектура?', 'Russian'),
    ('Які є джерела даних?', 'Ukrainian'),
    ('Quelle est cette architecture?', 'the language of the question'),
])
def test_answer_language_is_explicit_per_question(question, language):
    client = Mock()
    client.api_client.do.return_value = {"choices": [{"message": {"content": json.dumps(
        dict(supported=True, answer="Supported answer.", source_ids=[1]))}}]}
    retrieval.answer(client, 'model', question, [row()])
    system = client.api_client.do.call_args.kwargs['body']['messages'][0]['content']
    assert f'Required response language: {language}.' in system
