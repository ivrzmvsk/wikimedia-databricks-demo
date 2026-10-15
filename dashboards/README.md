# Gold Genie Wiki Analytics

Migrated on 2026-10-15 from `azure-dev` to `personal-env` (`ivan_personal`).

- Source dashboard: `01f1a779e0b81eab91ba35a93bfe4671`.
- Created dashboard: `01f1c89088fe15fe985a64a245090eda`.
- [Open published dashboard](https://dbc-ab9a5afe-c151.cloud.databricks.com/dashboardsv3/01f1c89088fe15fe985a64a245090eda/published).
- SQL warehouse: `6fec7d20a958edd1` (Serverless Starter Warehouse).
- Target tables: `workspace.wikimediademo_gold`.

`gold_genie_wiki_analytics.source.lvdash.json` contains the original editable
(draft) definition exported with `databricks lakeview get` from the dashboard
linked by the user. The API exports the draft definition; parity with the
source's last published revision was not independently verified.

`gold_genie_wiki_analytics.personal.lvdash.json` preserves the page, seven widgets,
nine datasets, IDs, layout, and UI settings. Only SQL catalog references were
changed from `dbr_dev` to `workspace`. All nine dataset SELECT queries executed
successfully and returned data in the target workspace before creation.
The target was published with individual data permissions (`embed_credentials=false`).

To reuse the definition in another workspace, import the `.lvdash.json` file
through the Databricks dashboard UI, select a SQL warehouse, and adjust catalog
and schema references in the datasets. These exports are standalone snapshots.

## Azure trial 2

Created and published on 2026-10-15 using profile `azure_trial_2`.

- [Open published dashboard](https://adb-7405618361895520.0.azuredatabricks.net/dashboardsv3/01f1c892348b11d7b333f8ac6a790393/published?o=7405618361895520).
- Dashboard ID: `01f1c892348b11d7b333f8ac6a790393`.
- SQL warehouse: `d8b8ff9942968cb8` (Serverless Starter Warehouse).
- Tables: `dbr_dev.wikimedia_prod_gold`.
- Export: `gold_genie_wiki_analytics.azure_trial_2.lvdash.json`.

The definition matches the source snapshot, including all seven widgets and nine
datasets. All nine SELECT queries succeeded and returned data in Azure trial 2.
The saved definition was read back and compared to the local export after
creation. Publication uses individual data permissions (`embed_credentials=false`).

Updated and republished on 2026-10-15 after PROD table isolation. All nine dataset
queries now use `dbr_dev.wikimedia_prod_gold`; each SELECT succeeded and returned
data before publication. The saved draft was read back and matched the expected
definition, preserving the layout and widget IDs. The publication API confirmed
a newer revision with the same warehouse and individual data permissions.

## Language views

Both personal and Azure trial 2 dashboards were updated and republished on
2026-10-15 to exclude `language_code IN ('commons', 'wikidata')` from all five
language-specific datasets, including the nested top-language selection. These
project codes previously appeared as `ISO: COMMONS` and `ISO: WIKIDATA`. Overall
metrics and hourly activity still include their events. This is a dashboard
filter; the dimension values and historical events remain unchanged.

All ten affected SELECTs succeeded and their result rows contained neither
label. Saved definitions matched the updates and publication metadata confirmed
new revisions. Layout, widget IDs, warehouses, and data permission settings were
preserved. The two target exports include the filters; the source export remains
the original migration snapshot.
