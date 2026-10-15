"""Package only app sources and a sanitized READY corpus report (never the repository)."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil


ROOT = Path(__file__).resolve().parents[1]


def document_snapshot(document):
    relative = document.get("source_path", "")
    if not re.fullmatch(r"rag_docs/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.md", relative):
        raise ValueError("Invalid document source path")
    path = ROOT / relative
    if path.is_symlink() or not path.resolve().is_relative_to((ROOT / "rag_docs").resolve()):
        raise ValueError("Invalid document source path")
    text = path.read_text(encoding="utf-8")
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != document["content_hash"]:
        raise ValueError("Document changed since ingestion; prepare and sync again before packaging")
    return relative, text


def package(report_file, destination, verify_on_start=False):
    report = json.loads(Path(report_file).read_text())
    if report.get("status") != "READY" or not report.get("sync", {}).get("update_id"):
        raise ValueError("A real successful sync report is required")
    config = {k: report[k] for k in ("status", "corpus_id", "generation", "start", "end", "prepared_at", "documents")}
    # Explicit allowlist: operator contacts, raw responses and event metadata never reach the App.
    snapshots = [document_snapshot(d) for d in report["documents"] if d.get("document_type") == "project_document"]
    config["documents"] = [{k: d[k] for k in ("canonical_title", "source_url", "fetched_at", "revision",
                                           "document_type", "source_path", "content_hash") if k in d}
                           for d in config["documents"]]
    config["verify_on_start"] = verify_on_start
    destination = Path(destination).resolve()
    allowed = (ROOT / "rag/application/assistant").resolve()
    if destination != allowed:
        raise ValueError(f"Destination must be {allowed}")
    destination.mkdir(parents=True, exist_ok=True)
    docs_dir = destination / "documents"
    if docs_dir.exists():
        shutil.rmtree(docs_dir)
    for relative, text in snapshots:
        path = docs_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    for name in ("app.py", "app.yaml", "retrieval.py", "launch.py", "requirements.txt"):
        shutil.copy2(ROOT / "app/wikimedia_assistant" / name, destination / name)
    (destination / "runtime.json").write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    parser.add_argument("--destination", default=str(ROOT / "rag/application/assistant"))
    parser.add_argument("--verify-on-start", action="store_true")
    args = parser.parse_args()
    package(args.report, args.destination, args.verify_on_start)
