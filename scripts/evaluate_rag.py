"""Small explicit real-service eval; never run automatically on PRs."""
import argparse
import json
from pathlib import Path
import re
import sys

from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app/wikimedia_assistant"))
from retrieval import answer, retrieve


def evaluate(client, config, cases, index, model):
    urls = {d["canonical_title"]: d["source_url"] for d in config["documents"]}
    results = []
    for case in cases:
        try:
            expected = {urls[t] for t in case["expected_titles"]}
            alternatives = {urls[t] for t in case.get("expected_any_titles", [])}
            rows = retrieve(client, index, case["question"], config["corpus_id"], config["generation"])
            result = answer(client, model, case["question"], rows)
            found = {s["url"] for s in result["sources"]}
            passed = (result["supported"] == case["supported"] and expected.issubset(found)
                      and (not alternatives or bool(alternatives & found))
                      and all(term.casefold() in result["answer"].casefold()
                              for term in case.get("answer_contains", [])))
            if case.get("answer_language") == "ru":
                passed = passed and len(re.findall(r"[А-Яа-яЁё]", result["answer"])) >= 10
            if case.get("answer_language") == "en":
                passed = passed and not re.search(r"[А-Яа-яЁё]", result["answer"])
            results.append(dict(**case, passed=passed, retrieved_titles=[r["title"] for r in rows], result=result))
        except Exception as exc:
            results.append(dict(**case, passed=False, error=str(exc)))
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    parser.add_argument("--cases", default="rag/demo_eval.json")
    parser.add_argument("--output", default=".state/rag-curated-demo/eval-results.json")
    parser.add_argument("--profile", default="personal-env")
    parser.add_argument("--index", default="workspace.wikimediademo_ai.wikimedia_summary_index")
    parser.add_argument("--model", default="databricks-gemma-3-12b")
    args = parser.parse_args()
    config = json.loads(Path(args.report).read_text())
    if config.get("status") != "READY":
        raise ValueError("Sync must finish before evaluation")
    results = evaluate(WorkspaceClient(profile=args.profile), config,
                       json.loads(Path(args.cases).read_text()), args.index, args.model)
    Path(args.output).write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    for r in results:
        print(json.dumps(r, ensure_ascii=False), flush=True)
    print(f"Passed: {sum(r['passed'] for r in results)}/{len(results)}")
    if not all(r["passed"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
