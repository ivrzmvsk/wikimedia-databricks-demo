# PROD validation — 2026-10-15

Target: `azure_trial_2`, workspace `7405618361895520`, catalog `dbr_dev`.
Deployment uses the private workspace root and the three isolated schemas
described in [prod-isolation.md](prod-isolation.md).

| Job | Run ID | Result |
| --- | --- | --- |
| Setup | `198996968129091` | SUCCESS |
| Reference | `1025920898982969` | SUCCESS |
| Events | `958569460549354` | SUCCESS |

The existing three job IDs and two pipeline IDs were retained. Processing and
alert schedules remain paused; setup is manual.

| New table | Rows |
| --- | ---: |
| `wikimedia_prod_bronze.raw_events` | 132,677 |
| `wikimedia_prod_silver.silver_wikipedia_edits` | 132,525 |
| `wikimedia_prod_silver.silver_wikipedia_edits_quarantine` | 152 |
| `wikimedia_prod_gold.fact_wikipedia_edits` | 132,525 |
| `wikimedia_prod_gold.dim_project` | 1,074 |

Coverage checks found zero missing expected clean event keys, zero missing
previous Silver keys, zero duplicate Silver keys, zero missing quarantine rows,
and zero missing historical payloads. Reference freshness passed. Gold masking
and row filtering reference functions in the new Gold schema.

Legacy table IDs and pipeline ownership were unchanged. Legacy Bronze, Silver,
and Gold each retained 125,667 rows. Old reference history remains in legacy
tables; the new reference history starts with the current API snapshot.

Producer defaults for `--target azure_trial_2` now use
`dbr_dev.wikimedia_prod_bronze.raw_events`. Running producer processes must be
restarted to load the updated configuration. This validation covers the migrated
raw snapshot and processing jobs, not a new live producer send.

The existing PROD dashboard `01f1c892348b11d7b333f8ac6a790393` was updated and
republished against `dbr_dev.wikimedia_prod_gold`. All nine dataset SELECTs
succeeded and returned data. Layout and widget IDs were preserved. The saved
definition matched the expected query changes; publication metadata confirmed a
new revision with the same warehouse and `embed_credentials=false`.

Local verification artifacts are under the ignored
`.state/prod-three-jobs-validation/` directory, including run results, raw snapshot
coverage, final verification, and dashboard verification.
