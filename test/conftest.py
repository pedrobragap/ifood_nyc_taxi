import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a Spark session for testing.
    This fixture is session-scoped, meaning it will be created once for the entire test session.
    """
    spark = (SparkSession.builder
            .master("local[2]")
            .appName("pytest")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
    
    yield spark
    
    # Cleanup after tests
    spark.stop() 