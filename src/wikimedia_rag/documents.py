"""Bounded Markdown ingestion into a combined, versioned RAG collection."""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from urllib.parse import quote

from wikimedia_rag.corpus import digest, generation


def load_documents(root, fetched_at):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("A real rag_docs directory is required")
    paths = sorted(root.rglob("*.md"))
    if not 1 <= len(paths) <= 50:
        raise ValueError("Expected 1..50 Markdown documents")
    documents, total = [], 0
    for path in paths:
        if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("Document symlinks are not supported")
        size = path.stat().st_size
        total += size
        if size > 256_000 or total > 2_000_000:
            raise ValueError("Markdown input exceeds bounded document size")
        raw = path.read_text(encoding="utf-8")
        body, title = raw, path.stem
        if raw.startswith("---\n"):
            parts = raw.split("\n---\n", 1)
            if len(parts) != 2:
                raise ValueError(f"Unclosed front matter: {path.name}")
            metadata, body = parts
            match = re.search(r"^title:\s*(.+)$", metadata, re.M)
            if match:
                title = match[1].strip().strip('\"\'')
        if not body.strip():
            raise ValueError(f"Empty Markdown document: {path.name}")
        relative = "rag_docs/" + path.relative_to(root).as_posix()
        if not re.fullmatch(r"rag_docs/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.md", relative):
            raise ValueError("Use simple Markdown paths with letters, numbers, underscores and hyphens")
        hashed = digest(raw)
        documents.append(dict(document_id="project-doc:" + relative, canonical_title=title,
                              source_path=relative, source_url="projectdoc:///" + quote(relative, safe="/"),
                              document_type="project_document", revision=hashed, content_hash=hashed,
                              fetched_at=fetched_at, text=body.strip()))
    return documents


def sections(text):
    """Headings inside fenced code are data, not Markdown section boundaries."""
    heading, lines, fence = [], [], None
    for line in text.splitlines():
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            char = marker[1][0]
            fence = None if fence == char else char if fence is None else fence
        match = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", line) if fence is None else None
        if match:
            if lines:
                yield " > ".join(heading), "\n".join(lines).strip()
            level = len(match[1])
            heading = heading[:level - 1] + [match[2]]
            lines = []
        else:
            lines.append(line)
    if lines:
        yield " > ".join(heading), "\n".join(lines).strip()


def blocks(text):
    """Keep fenced SQL blocks with their short preceding caption when possible."""
    result, lines, fence = [], [], None
    for line in text.splitlines():
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            char = marker[1][0]
            fence = None if fence == char else char if fence is None else fence
        if not line.strip() and fence is None:
            if lines:
                result.append("\n".join(lines))
                lines = []
        else:
            lines.append(line)
    if lines:
        result.append("\n".join(lines))
    combined = []
    for block in result:
        if block.startswith(("```", "~~~")) and combined and len(combined[-1]) < 200:
            combined[-1] += "\n\n" + block
        else:
            combined.append(block)
    return combined


def document_chunks(document, corpus, start, end, size=1800):
    chunks = []
    for heading, body in sections(document["text"]):
        if not body:
            continue
        prefix = f"Project documentation: {document['canonical_title']}\nFile: {document['source_path']}\nSection: {heading}\n\n"
        room = size - len(prefix)
        if room < 100:
            raise ValueError("Document title/heading is too long")
        pieces, pending = [], ""
        for block in blocks(body):
            if pending and len(pending) + len(block) + 2 > room:
                pieces.append(pending)
                pending = ""
            while len(block) > room:
                cut = block.rfind("\n", 0, room)
                if cut < room // 2:
                    cut = block.rfind(" ", 0, room)
                if cut < room // 2:
                    cut = room
                pieces.append(block[:cut])
                block = block[cut:].lstrip()
            pending = (pending + "\n\n" + block).strip()
        if pending:
            pieces.append(pending)
        for piece in pieces:
            text = prefix + piece
            chunks.append(dict(chunk_id=digest(f"{corpus}|{document['document_id']}|{len(chunks)}"),
                               document_id=document["document_id"], corpus_id=corpus, text=text,
                               content_hash=digest(text), source_url=document["source_url"],
                               title=document["canonical_title"], document_type="project_document",
                               fetched_at=document["fetched_at"], revision=document["revision"],
                               start_utc=start, end_utc=end))
    if not chunks:
        raise ValueError("Document has no usable section text")
    return chunks


def prepare_documents(spark, *, catalog, ai_schema, root, base_corpus_id):
    from pyspark.sql import functions as F
    from wikimedia_rag.storage import Store

    now = datetime.now(timezone.utc).isoformat()
    docs = load_documents(root, now)
    store = Store(spark, catalog, ai_schema)
    base = store.get_manifest(base_corpus_id)
    if base["status"] != "READY" or any(d.get("document_type") == "project_document" for d in base["documents"]):
        raise ValueError("Base must be a READY Wikipedia collection")
    corpus = digest(json.dumps([base_corpus_id, base["generation"],
                               [(d["source_path"], d["content_hash"]) for d in docs]]))[:24]
    try:
        previous = store.get_manifest(corpus)
    except RuntimeError as exc:
        if str(exc) != "Corpus not prepared":
            raise
        previous = {}
    saved = {d["document_id"]: d for d in previous.get("documents", [])}
    for doc in docs:
        old = saved.get(doc["document_id"])
        if old and old["content_hash"] == doc["content_hash"]:
            doc["fetched_at"] = old["fetched_at"]
    report = dict(corpus_id=corpus, start=base["start"], end=base["end"], prepared_at=now,
                  base_corpus_id=base_corpus_id, base_generation=base["generation"],
                  selection_mode="wikipedia_and_project_documents", http_attempts=0,
                  article_limit=len(base["documents"]), project_document_count=len(docs),
                  documents=[dict(d, document_type="wikipedia_summary") for d in base["documents"]] +
                  [{k: v for k, v in d.items() if k != "text"} for d in docs])
    store.manifest(report, "PREPARING")
    try:
        chunks = [r.asDict() for r in spark.table(f"{store.prefix}.knowledge_chunks").filter(
            (F.col("corpus_id") == base_corpus_id) & (F.col("generation") == base["generation"])).collect()]
        if (len(chunks) != base["chunk_count"] or {c["document_id"] for c in chunks}
                != {d["document_id"] for d in base["documents"]}):
            raise RuntimeError("Base Wikipedia snapshot is incomplete")
        for chunk in chunks:
            chunk.update(chunk_id=digest(f"{corpus}|{chunk['chunk_id']}"), corpus_id=corpus)
        chunks += [c for d in docs for c in document_chunks(d, corpus, base["start"], base["end"])]
        version = generation(chunks)
        for chunk in chunks:
            chunk["generation"] = version
        report.update(generation=version, chunk_count=len(chunks), document_count=len(report["documents"]))
        store.reconcile_chunks(corpus, chunks)
        store.manifest(report, "PREPARED")
        return report
    except Exception as exc:
        report["error"] = str(exc)
        store.manifest(report, "FAILED")
        raise
