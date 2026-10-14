from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_workflow_has_valid_paths_and_orders_all_gold_dependencies():
    workflow = yaml.safe_load((ROOT / "resources/workflow.yml").read_text())
    job = workflow["resources"]["jobs"]["wikimedia_medallion"]
    tasks = {task["task_key"]: task for task in job["tasks"]}

    def ancestors(key):
        parents = {item["task_key"] for item in tasks[key].get("depends_on", [])}
        return parents.union(*(ancestors(parent) for parent in parents))

    assert {"batch_medallion", "streaming_medallion", "dim_date",
            "dim_project", "dim_editor_type"} <= ancestors("fact_wikipedia_edits")
    assert "check_gold" in ancestors("dashboard_queries")
    for task in tasks.values():
        if "notebook_task" in task:
            assert (ROOT / "resources" / task["notebook_task"]["notebook_path"]).is_file()


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
        "/Workspace/Shared/.bundle/${bundle.name}/${bundle.target}"
    )
    assert azure["variables"]["catalog"] == "dbr_dev"
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
