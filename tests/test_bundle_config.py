from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_event_job_is_independent_of_reference_refresh_and_initialization():
    jobs = yaml.safe_load((ROOT / "resources/workflow.yml").read_text())["resources"]["jobs"]
    assert set(jobs) == {"wikimedia_medallion", "wikimedia_reference", "wikimedia_setup"}
    events = {task["task_key"]: task for task in jobs["wikimedia_medallion"]["tasks"]}
    assert set(events) == {
        "process_events_to_silver", "refresh_gold_edit_facts", "validate_gold_edit_facts"
    }
    assert {d["task_key"] for d in events["refresh_gold_edit_facts"]["depends_on"]} == {
        "process_events_to_silver"
    }
    assert events["validate_gold_edit_facts"]["depends_on"] == [{"task_key": "refresh_gold_edit_facts"}]
    assert not any("run_job_task" in task for task in events.values())
    assert jobs["wikimedia_medallion"]["schedule"]["quartz_cron_expression"] == '0 0 * * * ?'

    reference = jobs["wikimedia_reference"]
    tasks = {task["task_key"]: task for task in reference["tasks"]}
    assert set(tasks) == {"refresh_project_reference", "refresh_gold_project_dimension", "validate_gold_project_dimension"}
    assert tasks["refresh_gold_project_dimension"]["depends_on"] == [{"task_key": "refresh_project_reference"}]
    assert tasks["validate_gold_project_dimension"]["depends_on"] == [{"task_key": "refresh_gold_project_dimension"}]
    assert reference["schedule"]["quartz_cron_expression"] == '0 0 3 * * ?'

    setup = jobs["wikimedia_setup"]
    assert "schedule" not in setup
    tasks = {task["task_key"]: task for task in setup["tasks"]}
    assert set(tasks) == {"create_schemas", "create_event_inbox", "refresh_static_dimensions", "prepare_gold_fact_and_policies"}
    assert tasks["create_event_inbox"]["depends_on"] == [{"task_key": "create_schemas"}]
    assert tasks["refresh_static_dimensions"]["depends_on"] == [{"task_key": "create_schemas"}]
    assert {d["task_key"] for d in tasks["prepare_gold_fact_and_policies"]["depends_on"]} == {
        "create_event_inbox", "refresh_static_dimensions"
    }
    assert not any("pipeline_task" in task for task in setup["tasks"])

    for job in jobs.values():
        assert not any("run_job_task" in task for task in job["tasks"])
        assert job["email_notifications"] == "${var.job_email_notifications}"
        assert job["max_concurrent_runs"] == 1
        for task in job["tasks"]:
            if "notebook_task" in task:
                assert (ROOT / "resources" / task["notebook_task"]["notebook_path"]).is_file()


def test_reference_freshness_alert_uses_published_dimension():
    alerts = yaml.safe_load((ROOT / "resources/alerts.yml").read_text())["resources"]["alerts"]
    alert = alerts["wikimedia_reference_freshness"]
    assert "dim_project" in alert["query_text"]
    assert "MAX(gold_updated_at) IS NULL" in alert["query_text"]
    assert "INTERVAL 48 HOURS" in alert["query_text"]
    assert alert["evaluation"]["notification"] == "${var.alert_notifications}"
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    notification = bundle["variables"]["alert_notifications"]["default"]
    # The SQL Alerts API rejects a notification containing only empty subscriptions.
    assert notification["retrigger_seconds"] > 0
    assert notification["subscriptions"] == []


def test_targets_use_requested_profiles_and_catalogs():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert set(bundle["targets"]) == {"ivan_personal", "azure_trial_2"}
    ivan_personal = bundle["targets"]["ivan_personal"]
    azure = bundle["targets"]["azure_trial_2"]
    assert ivan_personal["default"] is True
    assert ivan_personal["workspace"]["profile"] == "personal-env"
    assert bundle["variables"]["catalog"]["default"] == "workspace"
    assert azure["workspace"]["profile"] == "azure_trial_2"
    assert azure["workspace"]["root_path"] == (
        "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}"
    )
    assert azure["variables"]["catalog"] == "dbr_dev"
    assert azure["variables"]["bronze_schema"] == "wikimedia_prod_bronze"
    assert azure["variables"]["silver_schema"] == "wikimedia_prod_silver"
    assert azure["variables"]["gold_schema"] == "wikimedia_prod_gold"
    assert azure["variables"]["raw_events_table"] == "raw_events"
    assert azure["presets"]["trigger_pause_status"] == "PAUSED"
    assert ivan_personal["mode"] == "development"
    assert "mode" not in azure
    assert bundle["variables"]["schedule_pause_status"]["default"] == "PAUSED"


def test_pipelines_ship_shared_sources_and_use_bundle_configuration():
    config = yaml.safe_load((ROOT / "resources/pipelines.yml").read_text())
    for pipeline in config["resources"]["pipelines"].values():
        assert pipeline["configuration"]["wikimedia.repo_root"] == "${workspace.file_path}"
        assert pipeline["configuration"]["wikimedia.catalog"] == "${var.catalog}"
        for library in pipeline["libraries"]:
            assert (ROOT / "resources" / library["file"]["path"]).is_file()


def test_event_pipeline_has_only_the_direct_inbox_to_silver_library():
    config = yaml.safe_load((ROOT / "resources/pipelines.yml").read_text())
    pipeline = config["resources"]["pipelines"]["wikimedia_streaming_medallion"]
    assert pipeline["libraries"] == [
        {"file": {"path": "../pipelines/streaming_medallion/silver_wikipedia_edits.py"}}
    ]
