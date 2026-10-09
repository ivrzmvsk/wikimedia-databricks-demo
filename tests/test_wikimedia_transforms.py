import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim


@pytest.fixture(scope="session")
def spark():
    """local SparkSession for tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("wikimedia-local-testing")
        .getOrCreate()
    )


def clean_page_titles(df):
    """An example of the article title cleaning function Wikimedia."""
    return df.filter(col("title").isNotNull()).withColumn(
        "cleaned_title", lower(trim(col("title")))
    )


def test_wikimedia_title_cleaning(spark):
    data = [("Main_Page", 100), (" Python_Data ", 50), (None, 10)]
    columns = ["title", "views"]
    df = spark.createDataFrame(data, columns)

    result_df = clean_page_titles(df)
    results = [row.cleaned_title for row in result_df.collect()]

    # Test: null filtr, cut the spaces, lower case
    assert len(results) == 2
    assert "python_data" in results
    assert "main_page" in results
