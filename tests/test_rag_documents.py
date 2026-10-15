from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import importlib.util
import json

import pytest

from wikimedia_rag.corpus import digest
from wikimedia_rag.documents import document_chunks, load_documents, prepare_documents

ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-10-15T09:00:00Z"


def test_markdown_chunks_keep_sections_sql_and_complete_body(tmp_path):
    query = "```sql\nSELECT title, COUNT(*) FROM facts\nGROUP BY title;\n```"
    raw = "---\ntitle: Project Guide\ntopic: demo\n---\n# Guide\n\nIntro.\n\n## SQL recipes\n\n" + query + "\n\n## Details\n\n" + "bounded text " * 350
    (tmp_path / "guide.md").write_text(raw)
    doc = load_documents(tmp_path, NOW)[0]
    chunks = document_chunks(doc, "corpus", NOW, NOW)
    assert doc["canonical_title"] == "Project Guide" and doc["content_hash"] == digest(raw)
    assert all(len(c["text"]) <= 1800 for c in chunks)
    assert any(query in c["text"] and "Guide > SQL recipes" in c["text"] for c in chunks)
    assert all("rag_docs/guide.md" in c["text"] for c in chunks)
    assert len({c["chunk_id"] for c in chunks}) == len(chunks)
    assert " ".join(c["text"].split("\n\n", 1)[1] for c in chunks).count("bounded text") == 350
    assert load_documents(tmp_path, NOW)[0] == doc


@pytest.mark.parametrize("content", ["", "---\ntitle: bad\n", "---\ntitle: empty\n---\n"])
def test_empty_or_malformed_docs_fail(tmp_path, content):
    (tmp_path / "bad.md").write_text(content)
    with pytest.raises(ValueError):
        load_documents(tmp_path, NOW)


def test_document_symlinks_and_oversized_inputs_fail(tmp_path):
    original = tmp_path / "source.md"
    original.write_text("# Source\nText")
    link = tmp_path / "link.md"
    link.symlink_to(original)
    with pytest.raises(ValueError, match="symlinks"):
        load_documents(tmp_path, NOW)
    link.unlink()
    original.write_text("x" * 256001)
    with pytest.raises(ValueError, match="size"):
        load_documents(tmp_path, NOW)


def test_combined_snapshot_preserves_wikipedia_and_repeat_generation(spark, tmp_path, monkeypatch):
    from wikimedia_rag import storage
    from wikimedia_rag.storage import CHUNK_SCHEMA
    (tmp_path / "guide.md").write_text("# Project\n\n## Operations\n\nEvents never call SiteMatrix.")
    source = dict(chunk_id="original", document_id="enwiki:1", corpus_id="base", text="Wikipedia source",
                  content_hash="hash", source_url="https://en.wikipedia.org/wiki/Example", title="Example",
                  document_type="wikipedia_summary", fetched_at=NOW, revision="1", start_utc=NOW,
                  end_utc=NOW, generation="base-version")
    frame = spark.createDataFrame([source], CHUNK_SCHEMA)
    proxy = SimpleNamespace(table=lambda _: frame)
    base = dict(status="READY", generation="base-version", chunk_count=1, start=NOW, end=NOW,
                documents=[dict(document_id="enwiki:1", canonical_title="Example", source_url=source["source_url"],
                                content_hash="hash", fetched_at=NOW, revision="1")])
    saved = {"base": base}
    store = Mock(prefix="workspace.ai")

    def get_manifest(corpus):
        if corpus not in saved:
            raise RuntimeError("Corpus not prepared")
        return saved[corpus]

    def manifest(report, status):
        report["status"] = status
        saved[report["corpus_id"]] = json.loads(json.dumps(report))

    store.get_manifest.side_effect = get_manifest
    store.manifest.side_effect = manifest
    monkeypatch.setattr(storage, "Store", lambda *args: store)
    first = prepare_documents(proxy, catalog="workspace", ai_schema="ai", root=tmp_path, base_corpus_id="base")
    chunks = store.reconcile_chunks.call_args.args[1]
    assert first["document_count"] == 2 and first["http_attempts"] == 0
    assert next(c for c in chunks if c["document_id"] == "enwiki:1")["text"] == "Wikipedia source"
    repeated = prepare_documents(proxy, catalog="workspace", ai_schema="ai", root=tmp_path, base_corpus_id="base")
    assert repeated["corpus_id"] == first["corpus_id"] and repeated["generation"] == first["generation"]
    assert store.reconcile_chunks.call_args.args[1] == chunks
    (tmp_path / "guide.md").write_text("# Updated\n\nChanged documentation.")
    changed = prepare_documents(proxy, catalog="workspace", ai_schema="ai", root=tmp_path, base_corpus_id="base")
    assert changed["corpus_id"] != first["corpus_id"]
    assert changed["base_corpus_id"] == "base"


def test_packaged_document_must_match_ingested_version(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("package", ROOT / "scripts/package_rag_app.py")
    package = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(package)
    monkeypatch.setattr(package, "ROOT", tmp_path)
    docs = tmp_path / "rag_docs"
    docs.mkdir()
    (docs / "guide.md").write_text("# Guide\nSnapshot.")
    document = dict(source_path="rag_docs/guide.md", content_hash=digest("# Guide\nSnapshot."))
    assert package.document_snapshot(document)[1] == "# Guide\nSnapshot."
    (docs / "guide.md").write_text("# Changed\nNew version.")
    with pytest.raises(ValueError, match="changed since ingestion"):
        package.document_snapshot(document)
    with pytest.raises(ValueError, match="source path"):
        package.document_snapshot(dict(document, source_path="rag_docs/../secret.md"))
