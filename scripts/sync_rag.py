"""Low-capacity DEV fallback: sync without holding a serverless notebook open."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from wikimedia_rag.corpus import identifier
from wikimedia_rag.search import sync_and_wait


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    parser.add_argument("--profile", default="personal-env")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--ai-schema", default="wikimediademo_ai")
    parser.add_argument("--index", default="workspace.wikimediademo_ai.wikimedia_summary_index")
    parser.add_argument("--embedding-endpoint", default="databricks-qwen3-embedding-0-6b")
    parser.add_argument("--warehouse-id", required=True)
    args = parser.parse_args()
    report = json.loads(Path(args.report).read_text())
    if report.get("status") not in ("PREPARED", "READY"):
        raise ValueError("A prepared corpus report is required")
    prefix = f"{identifier(args.catalog)}.{identifier(args.ai_schema)}"
    client = WorkspaceClient(profile=args.profile)
    report["sync"] = sync_and_wait(client, args.index, f"{prefix}.knowledge_chunks", args.embedding_endpoint)
    report["status"] = "READY"
    # Start SQL only AFTER the managed sync compute has terminated.
    parameters = [StatementParameterListItem(name=k, value=v, type="STRING") for k, v in {
        "report": json.dumps(report), "corpus": report["corpus_id"], "generation": report["generation"],
        "now": datetime.now(timezone.utc).isoformat()}.items()]
    statement = client.statement_execution.execute_statement(
        warehouse_id=args.warehouse_id, wait_timeout="0s", parameters=parameters,
        statement=f"""UPDATE {prefix}.corpus_manifest SET status='READY', report_json=:report,
            updated_at=:now WHERE corpus_id=:corpus AND generation=:generation""")
    deadline = time.monotonic() + 120
    try:
        while time.monotonic() < deadline:
            result = client.statement_execution.get_statement(statement.statement_id)
            state = result.status.state.value
            if state == "SUCCEEDED":
                Path(args.report).with_name("ready.json").write_text(json.dumps(report, indent=2))
                print(json.dumps(report["sync"]))
                return
            if state not in ("PENDING", "RUNNING"):
                raise RuntimeError(str(result.status))
            time.sleep(2)
        raise TimeoutError("Publishing ready manifest exceeded deadline")
    finally:
        if time.monotonic() >= deadline:
            client.statement_execution.cancel_execution(statement.statement_id)


if __name__ == "__main__":
    main()
