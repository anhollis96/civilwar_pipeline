import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("test-civilwar-pipeline").getOrCreate()

@pytest.fixture
def test_forces_data(spark):
    path='/Volumes/workspace/civilwar2/test_data/nm_forces_data.csv'
    test_forces_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)
    return(test_forces_df)

@pytest.fixture
def test_commanders_data(spark):
    path='/Volumes/workspace/civilwar2/test_data/nm_commanders_data.csv'
    test_commanders_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)
    return(test_commanders_df)
