import sys
from pathlib import Path
from pyspark import pipelines as dp

def add_repo_src_to_path() -> None:
    repo_root = spark.conf.get("wikimedia.repo_root", "")
    candidates = []

    if repo_root:
        candidates.append(Path(repo_root) / "src")
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2] / "src")

    candidates.extend([Path.cwd() / "src", Path.cwd().parent / "src"])

    for candidate in candidates:
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.append(candidate_str)

add_repo_src_to_path()

from wikimedia_batch.transforms import fetch_and_parse_sitematrix, add_batch_metadata

CATALOG_NAME = spark.conf.get("wikimedia.catalog", "dbr_dev")
BRONZE_SCHEMA_NAME = spark.conf.get("wikimedia.bronze_schema", "wikimediademo_bronze")
BRONZE_REFERENCE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA_NAME}.ref_wikimedia_projects"

@dp.table(
    name=BRONZE_REFERENCE_TABLE,
    comment="Reference dataset mapping Wikimedia project codes to readable names."
)
@dp.expect_all(
    {
        "valid_wiki_code": "wiki_code IS NOT NULL",
        "valid_project_name": "project_name IS NOT NULL"
    }
)
def ref_wikimedia_projects():
    parsed_rows = fetch_and_parse_sitematrix()
    
    df_raw = spark.createDataFrame(parsed_rows)
    
    return add_batch_metadata(df_raw)