import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

from wikimedia_batch.dq_rules import compute_dq_flags

def apply_batch_dq(spark, wiki_code="enwiki", site_url="https://en.wikipedia.org", project_name="Wikipedia", language_code="en"):
    """Creates a single-row DataFrame and applies the batch DQ rules."""
    
    schema = StructType([
        StructField("wiki_code", StringType(), True),
        StructField("site_url", StringType(), True),
        StructField("project_name", StringType(), True),
        StructField("language_code", StringType(), True)
    ])
    
    row = Row(
        wiki_code=wiki_code, 
        site_url=site_url, 
        project_name=project_name, 
        language_code=language_code
    )
    
    df = spark.createDataFrame([row], schema=schema)
    
    return compute_dq_flags(df)


def test_valid_reference_row_passes_dq(spark):
    row = apply_batch_dq(spark).collect()[0]
    
    assert row._dq_passed is True
    assert row._dq_failure_reasons == []

def test_missing_wiki_code_fails_dq(spark):
    row = apply_batch_dq(spark, wiki_code=None).collect()[0]
    
    assert row._dq_passed is False
    assert "valid_wiki_code" in row._dq_failure_reasons

def test_invalid_url_prefix_fails_dq(spark):
    row = apply_batch_dq(spark, site_url="www.wikipedia.org").collect()[0]
    
    assert row._dq_passed is False
    assert "valid_url" in row._dq_failure_reasons

def test_null_url_passes_dq(spark):
    row = apply_batch_dq(spark, site_url=None).collect()[0]
    
    assert row._dq_passed is True
    assert row._dq_failure_reasons == []