import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "sample_data", "flights_sample.csv")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("flightlake-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze(spark):
    """The sample file read the way bronze stores it: every column a string."""
    return spark.read.option("header", "true").csv(SAMPLE).cache()
