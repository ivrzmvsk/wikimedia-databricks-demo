# Jobs and pipeline responsibilities

There are three independent Databricks jobs and two Lakeflow pipelines. Jobs do
not start each other. Layer names describe tables, not separate jobs.

## Setup: `wikimedia_setup`

Manual only, on first installation or configuration/static-data/policy changes.

| Task | Responsibility |
| --- | --- |
| `create_schemas` | Create Bronze/Silver/Gold schemas if absent. |
| `create_event_inbox` | Create the sole Bronze event inbox if absent and grant the configured producer principal access. |
| `refresh_static_dimensions` | Build the date and editor-type dimensions. |
| `prepare_gold_fact_and_policies` | Create the empty fact table only if absent, then create/update and attach the user mask and row filter. Never replace existing fact data. |

Schemas run first; inbox and static dimensions can run in parallel. Gold and
governance wait for both. Setup does not fetch SiteMatrix, ingest events, create
pipeline-owned Silver tables, or invoke the processing jobs. Account groups and
the catalog must already exist.

## Events: `wikimedia_events`

For deployment continuity the bundle resource key remains `wikimedia_medallion`:
`databricks bundle run -t ivan_personal wikimedia_medallion`. The existing job ID
is retained; its display name is now `wikimedia_events_<target>`.

The external `producer/zerobus/producer.py` command captures Wikimedia SSE events
and writes `Bronze.zerobus_wikimedia_inbox` through Zerobus. This is the only
persisted Bronze event table. The Databricks job processes events already in it.

| Task | Responsibility | Outputs |
| --- | --- | --- |
| `process_events_to_silver` | Run the streaming pipeline: read inbox directly, parse JSON, calculate DQ flags, deduplicate clean events, and route rejected records. | `Silver.silver_wikipedia_edits`, `Silver.silver_wikipedia_edits_quarantine` |
| `refresh_gold_edit_facts` | Check required dimensions, then rebuild the fact table using Silver and the last published dimensions. Failed preflight does not replace existing facts. | `Gold.fact_wikipedia_edits` |
| `validate_gold_edit_facts` | Check dimension key uniqueness, project/date references, and equality of Silver/Gold row counts. | Pass/fail |

Tasks run in the listed order. There is no separate `check_dimensions` task.
The pipeline contains the temporary staging view and two Silver output tables;
it does not own or duplicate inbox. Existing Gold masks and row filters are
retained by replacement with the current fact column names.

The configured schedule is hourly and remains paused in DEV.

## Reference: `wikimedia_reference`

| Task | Responsibility | Outputs |
| --- | --- | --- |
| `refresh_project_reference` | Run the batch pipeline: fetch SiteMatrix, flatten its current snapshot into Bronze, validate rows, update SCD2 history, and separate rejected reference rows. | `Bronze.ref_wikimedia_projects`, `Silver.silver_ref_wikimedia_projects`, `Silver.silver_ref_quarantine` |
| `refresh_gold_project_dimension` | Publish current active project versions and the fallback project together. Reject empty/duplicate-key snapshots before replacing the previous dimension. | `Gold.dim_project` |
| `validate_gold_project_dimension` | Check publication coverage, unique project keys, and exactly one fallback project. | Pass/fail |

Tasks run in the listed order. The daily schedule is 03:00 Europe/Warsaw and
remains paused in DEV. API failures leave the previous published dimension
available to events. A separate SQL alert checks reference freshness after
48 hours; notification recipients must be configured.

## First installation and maintenance

Deploy, then run setup → reference → events manually, as described in
[setup instructions](manual-setup.md). After installation the processing jobs run
independently. Run setup separately when changing static data or policies, with
processing schedules paused during maintenance.

Dashboard SELECT examples remain manual queries. The one-time
[inbox cutover](inbox-cutover.md) is outside all regular jobs; rollback snapshots
are separate from the active layers.
