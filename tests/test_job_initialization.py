from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"


def run_notebook(name, sql):
    context = {"GOLD": "test.gold", "SILVER": "test.silver", "spark": SimpleNamespace(sql=sql)}
    exec(compile((NOTEBOOKS / name).read_text(), name, "exec"), context)


def test_event_preflight_accepts_existing_dimensions_without_calling_source_api():
    sql = Mock()
    sql.return_value.first.return_value = {
        "dates": 1000, "editor_types": 2, "projects": 900, "fallback_projects": 1
    }
    run_notebook("gold/04_fact_wikipedia_edits.py", sql)
    assert sql.call_count == 2
    assert "test.gold.dim_project" in sql.call_args_list[0].args[0]
    assert "CREATE OR REPLACE TABLE test.gold.fact_wikipedia_edits" in sql.call_args.args[0]


@pytest.mark.parametrize("dimension", ["dates", "editor_types", "projects", "fallback_projects"])
def test_event_preflight_rejects_uninitialized_dimensions(dimension):
    dimensions = dict(dates=1000, editor_types=2, projects=900, fallback_projects=1)
    dimensions[dimension] = 0
    sql = Mock()
    sql.return_value.first.return_value = dimensions
    with pytest.raises(RuntimeError, match="wikimedia_setup"):
        run_notebook("gold/04_fact_wikipedia_edits.py", sql)
    assert sql.call_count == 1  # No fact replacement after failed preflight.


def test_event_preflight_explains_missing_tables():
    with pytest.raises(RuntimeError, match="wikimedia_setup"):
        run_notebook("gold/04_fact_wikipedia_edits.py", Mock(side_effect=RuntimeError("TABLE_NOT_FOUND")))


@pytest.mark.parametrize("projects,unique", [(0, 0), (2, 1), (1, 0)])
def test_bad_reference_does_not_replace_last_published_dimension(projects, unique):
    sql = Mock()
    sql.return_value.first.return_value = {"projects": projects, "unique_projects": unique}
    with pytest.raises(RuntimeError, match="keeping dim_project"):
        run_notebook("gold/02_dim_project.py", sql)
    assert sql.call_count == 1
    assert "CREATE OR REPLACE" not in sql.call_args.args[0]


@pytest.mark.parametrize("field,value", [
    ("source_projects", 0), ("published_projects", 899),
    ("unique_keys", 900), ("fallback_projects", 0), ("missing_projects", 1),
])
def test_reference_checks_fail_on_incomplete_publication(field, value):
    counts = dict(source_projects=900, published_projects=900, unique_keys=901,
                  fallback_projects=1, missing_projects=0)
    counts[field] = value
    sql = Mock()
    sql.return_value.first.return_value = counts
    with pytest.raises(AssertionError, match="Reference publication checks failed"):
        run_notebook("check_reference.py", sql)
