import urllib.request
import json
from pyspark.sql import Row, DataFrame
from pyspark.sql import functions as F

def fetch_and_parse_sitematrix() -> list[Row]:
    """Fetches Wikimedia SiteMatrix API and flattens it into Spark Rows."""
    url = "https://en.wikipedia.org/w/api.php?action=sitematrix&format=json"
    req = urllib.request.Request(
        url, 
        headers={"User-Agent": "Databricks-Student-Sprint/1.0 (Educational Demo)"}
    )
    
    with urllib.request.urlopen(req) as response:
        raw_json_data = response.read().decode('utf-8')
    
    sitematrix = json.loads(raw_json_data).get("sitematrix", {})
    parsed_rows = []
    
    for key, val in sitematrix.items():
        if key == "count" or not isinstance(val, dict):
            continue
            
        if "site" in val:
            lang_code = val.get("code", "unknown")
            for site in val["site"]:
                parsed_rows.append(Row(
                    wiki_code=site.get("dbname"),
                    project_name=site.get("sitename"),
                    language_code=lang_code,
                    site_url=site.get("url")
                ))
        elif key == "specials":
            for site in val:
                parsed_rows.append(Row(
                    wiki_code=site.get("dbname"),
                    project_name=site.get("sitename"),
                    language_code=site.get("code", "special"),
                    site_url=site.get("url")
                ))
                
    return parsed_rows

def add_batch_metadata(df: DataFrame) -> DataFrame:
    """Appends rubric-required metadata columns."""
    return (
        df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_batch_run_id", F.lit("lakeflow-batch-pipeline"))
    )