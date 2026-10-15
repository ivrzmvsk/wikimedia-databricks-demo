"""Optional release smoke runs inside the App using its own OAuth principal."""
import json
import hashlib
from pathlib import Path
import subprocess
import sys
import os

from databricks.sdk import WorkspaceClient
from retrieval import answer, document_path, retrieve


config = json.loads(Path("runtime.json").read_text())
if config.get("verify_on_start"):
    client = WorkspaceClient()
    document = config["documents"][0]
    question = f"What is {document['canonical_title']} about?"
    rows = retrieve(client, os.environ["VECTOR_SEARCH_INDEX"], question,
                    config["corpus_id"], config["generation"])
    result = answer(client, os.environ["LLM_ENDPOINT"], question, rows)
    if not result["supported"] or document["source_url"] not in {s["url"] for s in result["sources"]}:
        raise RuntimeError("App service-principal grounding smoke failed")
    print("APP_SP_SMOKE " + json.dumps(result, ensure_ascii=False), flush=True)
    documents = [d for d in config["documents"] if d.get("document_type") == "project_document"]
    for document in documents:
        relative = document_path(document["source_url"])
        if not relative:
            raise RuntimeError("Invalid packaged document source")
        text = (Path("documents") / relative).read_text(encoding="utf-8")
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != document["content_hash"]:
            raise RuntimeError("Packaged source differs from indexed document")
    if documents:
        document = documents[0]
        rows = retrieve(client, os.environ["VECTOR_SEARCH_INDEX"], document["canonical_title"],
                        config["corpus_id"], config["generation"])
        if document["source_url"] not in {r["source_url"] for r in rows}:
            raise RuntimeError("App service-principal document retrieval failed")
        print("APP_DOCS_SMOKE " + json.dumps(dict(documents=len(documents),
              source=document["source_url"], packaged_hashes_match=True)), flush=True)
sys.exit(subprocess.call([sys.executable, "-m", "streamlit", "run", "app.py",
                          "--server.address=0.0.0.0", "--server.port=8000", "--server.headless=true"]))
