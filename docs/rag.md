# Wikimedia managed RAG

The current curated demo uses 40 articles plus 12 project Markdown documents.
See [the document guide](rag-project-documents.md) and [the article demo guide](rag-demo-40.md)
for the selection, example questions, validation results and App state. The
one-article deployment and pricing discussion below record the initial setup.

Status: the user authorized DEV deployment and a one-event smoke. See
`rag-dev-validation.md` for actual cloud results and current resource state.
This is separate from account billing status: AWS region alone does not establish
a paid subscription. No subscription or payment method is changed by these bundles.
Do not label mocked retrieval or LLM tests as a cloud quality evaluation.

## Design and isolation

The active platform remains Wikimedia SSE → bounded Zerobus producer → inbox →
Lakeflow Bronze → Silver → Gold. RAG reads `silver_wikipedia_edits` in a separate
serverless Lakeflow Job. No HTTP calls or RAG dependencies were added to Silver,
and the medallion Job does not depend on enrichment success.

Three small bundles separate lifecycle and release order:

- `rag/preparation`: standalone Job with `prepare`, `sync`, and `smoke` actions.
- `rag/serving`: managed DELTA_SYNC index on an existing STANDARD AI Search endpoint, automatic
  embeddings from `knowledge_chunks.text`, TRIGGERED synchronization.
- `rag/application`: Databricks App and its resource permissions. App creation and
  version deployment are separate commands and occur after completed sync.

These bundles use the direct deployment engine. The original bundle keeps its
current engine, state, pipelines, CI deployment policy, and Zerobus resources.
The installed CLI is 1.10.0; search resources require CLI >=1.1.0 and direct
engine per [Databricks documentation](https://docs.databricks.com/aws/en/ai-search/create-ai-search).
A shared root bundle was deliberately avoided because changing its engine would
require assessing migration of existing managed resources.

## Corpus contract

Supply an explicit UTC `[start,end)` interval. Filter `enwiki`, namespace 0,
`edit`/`new`, and nonempty titles. Group by wiki/title and rank by event count
DESC, title ASC. Neither Kafka columns nor Zerobus acknowledgment defines the
interval or data readiness. A successful upstream run and SQL visibility must
be confirmed before using a new ingestion session.

`article_limit` defaults to 1 and is restricted to 1..40. It counts **usable
canonical documents**, not attempts. `max_attempts` defaults to 1, counts all
HTTP requests including retries and redirects, and also bounds candidate titles.
One driver makes sequential requests (parallelism 1), at least 0.5 seconds apart;
default delay 1 second, timeout 10 seconds, retries 0, maximum backoff 60 seconds.
A 429 respects seconds or HTTP-date `Retry-After`; a longer delay fails the page
for a later job instead of retrying earlier. Unsafe HTTP redirects are rejected.
Wikipedia canonical title/pageid deduplicates aliases and redirects.

The HTTP API is the documented
[Wikipedia Summary REST endpoint](https://www.mediawiki.org/wiki/Wikimedia_REST_API/en).
Titles are fully URL encoded. User-Agent is `WikimediaAssistant/1.0 (<contact>)`;
defaults to this repository’s issue tracker and can be overridden with an
operator-controlled contact URL or email.
The [API etiquette](https://www.mediawiki.org/wiki/API:Etiquette) and
[rate-limit policy](https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits)
were checked before implementation. Missing/disambiguation pages, empty extracts,
malformed JSON and transport errors are recorded per candidate. HTTP runs on the
Job driver, never in Spark UDFs. An incomplete corpus fails the Job and is not
published as a successful refresh; successful individual summaries stay cached.

Within `<catalog>.<ai_schema>`:

| Table | Purpose |
| --- | --- |
| `article_content` | Canonical summary cache, extract, description, URL, revision, fetched time, response JSON, text hash |
| `article_aliases` | Requested title → canonical document ID |
| `knowledge_chunks` | Bounded text fragments and retrieval metadata; Delta CDF explicitly enabled |
| `corpus_manifest` | Selection interval/rules, candidate statuses, selected documents, generation, preparation/sync/smoke report |

The summary cache accumulates across sessions. Each **retrieval corpus is a
replacement snapshot** of its own interval and limit, identified by a stable
hash of normalized UTC bounds, rules and limit. Other corpus rows are preserved.
Within the selected corpus, MERGE inserts/updates changed chunks and deletes
obsolete chunk IDs; a repeated cached run does not rewrite unchanged rows.
Document IDs use wiki/pageid, chunk IDs use corpus/document/position.
Content hashes and embedding text omit technical timestamps. Event interval,
summary fetch time and article revision are distinct metadata fields.

The corpus generation fingerprints the actual chunks and their source versions.
App packages pin both corpus ID and generation. AI Search filters both before
retrieval; stale index rows or other sessions cannot enter that App's context.
Changing text, fetched version, or selected articles requires sync and a new App
package. The previous deployed App can serve its explicitly dated snapshot until
replacement; it does not advertise an unfinished refresh as current data.

Refresh policies: `missing` always reuses an available cache entry; `expired`
(default) refetches after `ttl_hours=168`; `force` refetches selected pages within
the same request budget. Cached fetch times stay unchanged when reused.

Do not edit an existing index's embedding model or source schema in place.
The sync action checks source, primary key, mode, embedding column and model.
A mismatch fails. Review a separately named new index, re-embed, then explicitly
switch the App; retain or remove the old resource only with authorization.

## Workspace observations before authorized deployment on 2026-10-15

Read-only API inspection of `personal-env` confirmed:

- AWS metastore region `us-east-2`; this is not covered by Azure credits.
- Silver exists with required columns and the existing streaming pipeline ID.
- Workflow run `667206744294497` and all tasks terminated successfully.
- An unrelated existing STANDARD search endpoint has two indexes; it is reused for the RAG index, and is not adopted or deleted by these bundles.
  Its existing indexes are unchanged.
- Managed `databricks-qwen3-embedding-0-6b` embedding and `databricks-gemma-3-12b` chat
  endpoints are READY. Defaults reuse these pay-per-token APIs and create no
  dedicated model serving endpoint. CAN_QUERY and real model responses still
  require verification during the authorized smoke.
- Qwen embeddings support multilingual questions against English summaries; the model
  is pinned per index. The English-only GTE endpoint is available but was not selected.
- The SQL warehouse `6fec7d20a958edd1` is STOPPED, auto-stop 10 minutes. It was
  not started for read-only inspection; current row counts were not recalculated.

The preserved SQL statement `01f1c82f-b2c1-130d-853a-e57a9b67ba66` was reread
without starting compute: 6 inbox and Bronze deliveries, 5 Silver events, zero
quarantine and duplicate keys for the last manifest. This is its saved result,
not a new current row-count query.

The saved latest manifest contains the eligible article `Surinam Airways`, event
UTC `2026-10-15T00:27:16Z`, and the saved downstream report accounts for all five
manifest IDs in Silver. Suggested first interval is
`[2026-10-15T00:27:16Z,2026-10-15T00:27:17Z)`. This is a concrete smoke candidate,
not a claim that its summary or current Silver selection has already been tested.
After approval, first confirm this selection with SQL/Job reads against current
Silver. Do not start another ingestion session unless this evidence is insufficient.

Both Zerobus readiness review issues are already addressed in current code:
SQL polling follows durability acknowledgment; capture has a parent-controlled
process deadline even for heartbeat-only SSE. Existing regression tests cover both.

## First release commands (only after approval)

Run from the repository root with Databricks CLI >=1.1.0. Set contact explicitly.
All three RAG bundles support only `ivan_personal` and pin the personal workspace
host. `azure_trial_2` is intentionally absent, so selecting that target fails
before deployment. Azure CI validates and deploys only the main medallion bundle;
it never deploys the RAG job, AI Search index or App.
Keep catalog/index/model values consistent between the three bundles.

```sh
export BUNDLE_VAR_contact='https://github.com/ivrzmvsk/wikimedia-databricks-demo/issues'
export BUNDLE_VAR_start='2026-10-15T00:27:16Z'
export BUNDLE_VAR_end='2026-10-15T00:27:17Z'
export BUNDLE_VAR_article_limit='1'
export BUNDLE_VAR_max_attempts='1'
mkdir -p .state/rag-smoke

(cd rag/preparation && databricks bundle validate -t ivan_personal)
(cd rag/preparation && databricks bundle plan -t ivan_personal)
(cd rag/preparation && databricks bundle deploy -t ivan_personal)
(cd rag/preparation && databricks bundle run -t ivan_personal wikimedia_rag --params action=prepare)

# Proceed only after PREPARED report confirms one usable canonical article.
(cd rag/serving && databricks bundle validate -t ivan_personal)
(cd rag/serving && databricks bundle plan -t ivan_personal)
(cd rag/serving && databricks bundle deploy -t ivan_personal)
(cd rag/preparation && databricks bundle run -t ivan_personal wikimedia_rag --params action=sync)
```

The sync action snapshots existing Lakeflow update IDs, triggers AI Search sync,
then waits for a **new** update to reach COMPLETED and the index to be ready.
An active/concurrent sync, model mismatch, FAILED/CANCELED update, or timeout
fails the Job. `ready=true` from an earlier update is never sufficient. This
uses owner access to the managed sync pipeline; App SP does not need that access.

Free compute can reject sync while the notebook driver holds capacity. If that
happens, stop this Job and the SQL warehouse started for the test; do not modify
other workloads. Once the managed pipeline is idle, use the local fallback:

```sh
.venv/bin/python scripts/sync_rag.py .state/rag-smoke/prepared.json \
  --warehouse-id 6fec7d20a958edd1
```

It waits for a new completed sync without a serverless notebook, then starts SQL
to publish the READY manifest and writes `.state/rag-smoke/ready.json`. Account
quotas still apply; it does not bypass quota or change the subscription.

Retrieve the sync task output (task run ID, not the parent multi-task run ID):

```sh
databricks jobs get-run PARENT_SYNC_RUN_ID -p personal-env -o json > .state/rag-smoke/sync-run.json
# Read tasks[0].run_id from that JSON and substitute it below.
databricks jobs get-run-output SYNC_TASK_RUN_ID -p personal-env -o json > .state/rag-smoke/sync-output.json
.venv/bin/python - <<'PY'
import json
from pathlib import Path
output = json.loads(Path('.state/rag-smoke/sync-output.json').read_text())
assert not output['notebook_output'].get('truncated', False)
report = json.loads(output['notebook_output']['result'])
assert report['status'] == 'READY'
Path('.state/rag-smoke/ready.json').write_text(json.dumps(report, indent=2))
PY
.venv/bin/python scripts/package_rag_app.py .state/rag-smoke/ready.json
(cd rag/application && databricks bundle validate -t ivan_personal)
(cd rag/application && databricks bundle plan -t ivan_personal)
(cd rag/application && databricks bundle deploy -t ivan_personal)
# This next command actually deploys/runs the App version; bundle deploy alone does not.
(cd rag/application && databricks bundle run -t ivan_personal wikimedia_assistant)
(cd rag/preparation && databricks bundle run -t ivan_personal wikimedia_rag --params action=smoke)
```

The App package allowlist contains only five application files plus sanitized
`runtime.json`. It excludes HTTP contacts, raw summaries, raw events, credentials,
usernames, notebooks and repository documentation. The package script requires a
READY report with a completed update ID. A local validation placeholder reports
NOT_DEPLOYED and makes the UI refuse to run; it is not a cloud artifact.

App resource grants: SELECT on the index (automatic USE CATALOG/USE SCHEMA grants
require deployer's MANAGE privileges), CAN_QUERY on the existing LLM and embedding
APIs. No Silver, cache, write or SQL warehouse permission is granted to the App.
See [App AI Search resources](https://docs.databricks.com/gcp/en/dev-tools/databricks-apps/vector-search).
If automatic grants fail, have the schema/index owner assign these exact rights.
Do not grant ALL PRIVILEGES to work around it.

## Smoke and evaluation

The smoke action refuses `article_limit != 1`. It uses real managed retrieval,
checks the expected article URL, invokes the LLM, requires valid source IDs,
checks refusal for an out-of-corpus question, reruns preparation using the cache,
and compares complete chunk rows plus Delta operation metrics. It requires zero
HTTP requests on repeat, no duplicate keys and no inserted/updated/deleted chunk
rows. Save its output before Jobs retention removes it.

Review factual correctness manually against the saved extract: citations alone
cannot establish that each claim is supported. Initial real-document eval cases:

| Question | Expected answer/source |
| --- | --- |
| What is `<canonical title>` about? | Main subject from its saved extract; exact saved source URL |
| What does the first sentence say about `<canonical title>`? | Paraphrase of the saved first sentence; same URL |
| What is the workspace password? | Refusal; no source |
| What is the exact number of edits tomorrow? | Refusal; no source |
| Why did a specific historical edit change this article? | Refusal unless that reason exists in supplied context |

The smoke report includes the grounded answer, expected source, out-of-corpus
answer, and idempotency evidence. Optional `package_rag_app.py --verify-on-start` runs a real grounded question
inside the App before Streamlit starts, proving App-SP retrieval/model rights.
It is disabled by default and does not run on PRs.
App UI smoke must additionally confirm app-SP
retrieval/model permissions, history, source buttons, timestamps and service
error behavior. Job-owner success does not prove app-SP permissions.

After the successful one-article smoke, separately expand `article_limit=10`
and the bounded attempt budget, and add an explicitly reviewed Markdown allowlist
covering architecture, byte_change, DQ/quarantine and operations. No documentation
is ingested in this first release. Stage two must supply sanitized documents and
stable repository source links; historical notebooks and whole-repository scans
are excluded. The one-article smoke gate is not bypassed by changing a default.

## Costs, stopping and recovery

**Free Edition is free within quotas**, including one Search endpoint and up to
three Apps. Quota exhaustion pauses compute instead of automatically charging
for overage. Free Trial is a different offer, with expiring credits and possible
PAYG conversion when a payment method/Marketplace subscription is attached.
See [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
and [Free Edition versus trial](https://docs.databricks.com/aws/en/getting-started/free-trial-vs-free-edition).
The prices below apply to paid accounts and do not establish this account's bill.
DEV reuses the existing Search endpoint, so it adds no second endpoint base unit.

Published prices were checked on 2026-10-15. Contract-specific Ohio price tiles
are dynamically rendered and were not independently obtainable in this session;
verify them in the account before deployment. These are estimates, not a hard
billing cap. Azure's $200 credits do not pay this AWS bill.

[AI Search pricing](https://www.databricks.com/product/pricing/ai-search) lists
STANDARD minimum 4 DBU/hour and a published US East reference price $0.28/hour
($0.07/DBU). At that rate: $6.72/day, $201.60/720-hour month for search alone.
One article limits embeddings, not minimum endpoint capacity.
[Cost management](https://docs.databricks.com/aws/en/ai-search/cost-management)
says endpoint billing begins with an index and stops 24 hours after removal of
the final index. Plan a possible $6.72 tail even after index cleanup; do not
promise immediate zero charges merely because the Job ended or no queries run.

[App Medium](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/compute-size)
uses 0.5 DBU/hour, billed while running. Formula: 0.5 × account App $/DBU × hours.
A $0.75/DBU planning assumption gives $0.375/hour ($270/720-hour month); this App
rate must be verified for the account. Stop the App after the agreed smoke window.

[Foundation model rates](https://www.databricks.com/product/pricing/foundation-model-serving):
Qwen 3 0.6B Embedding 0.286 DBU/million input tokens; Gemma 3 12B 2.143 input and 7.143 output
DBU/million tokens. At $0.07/DBU, roughly $0.02/M embedding tokens and $0.15/M
LLM input, $0.50/M output tokens. A one-article smoke is far below a dollar for
these token charges. Serverless Job runs, search ingestion, storage and any
warehouse start are additional and depend on runtime/account prices.

Proposed approval scope: one new standalone Job; AI schema with four Delta tables;
reuse of `ivanrazumovskyi-lab12-steam-dev` STANDARD endpoint; one
`workspace.wikimediademo_ai.wikimedia_summary_index`; one
`wikimedia-assistant-dev` Medium App; existing pay-per-token Qwen/Gemma.
Plan one active hour, a **$20 planning allowance** including cleanup tail and
serverless work, subject to checking Ohio/account prices. Stop on a cost estimate
that no longer fits this allowance. This is manual cost control; delayed billing
cannot enforce an exact dollar cap.

Agreed cleanup should stop the newly created App and destroy **only the new App and RAG-index bundles** after preserving reports. Never destroy the original bundle
or the unrelated existing search endpoint. These commands are documentation,
not actions already performed or blanket permission to delete existing data:

```sh
databricks apps stop wikimedia-assistant-dev -p personal-env
(cd rag/application && databricks bundle plan -t ivan_personal)
# Review state ownership and resource names before bundle destroy.
(cd rag/application && databricks bundle destroy -t ivan_personal)
(cd rag/serving && databricks bundle destroy -t ivan_personal)
```

Verify deletion via endpoint/index APIs and check subsequent billing. If cleanup
fails, report it immediately; do not leave resources running silently. Delta
cache/chunks/manifest and preparation Job may remain; they have no scheduled
compute, and table storage can still cost money. No table deletion is included.
To restore, deploy the index from code on the retained existing endpoint against retained CDF chunks, run explicit
sync, export a fresh READY report, package and deploy/run the App. Model changes
require an explicitly reviewed new index, not reuse of incompatible embeddings.

## Completed verification

- 95 local tests passed (52 existing and 43 new), including real local Spark
  selection and Zerobus heartbeat/visibility regressions; no network inference.
- Flake8, Python compilation and `git diff --check` passed.
- Existing medallion plus all three RAG bundles validated in `personal-env` with
  CLI 1.10.0. RAG plans contain a new Job, new index and new App, with no existing-resource deletes.
- Authorized DEV deployment and real one-event/one-article smoke passed.
  Managed retrieval, LLM answers/refusals, App-SP access and repeat idempotency
  are documented in `rag-dev-validation.md`. Account billing was not audited.
- User README changes and other pre-existing untracked files were preserved;
  no commit, new ingestion session, subscription upgrade or resource deletion occurred.
