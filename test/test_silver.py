import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, TimestampType, StringType, DateType
)
from pyspark.sql.functions import col
from src.utils import prepare_silver_df

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("pytest-silver")
    .getOrCreate()
)

def test_prepare_silver_df_filters_and_casts():
    data = [
        {"id": 1, "value": "10.5", "event_time": "2025-06-01 09:00:00"},
        {"id": 2, "value": "20.0", "event_time": "2025-07-01 12:30:00"},
        {"id": 3, "value": "30.0", "event_time": "2025-08-01 14:45:00"},
        {"id": 4, "value": "40.0", "event_time": "2025-05-31 23:59:59"},
    ]
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("value", StringType(), True),
        StructField("event_time", StringType(), True),
    ])
    df_bronze = spark.createDataFrame(data, schema)

    start_date = date(2025, 6, 1)
    end_date   = date(2025, 7, 31)

    df_silver = prepare_silver_df(spark, df_bronze, start_date, end_date).cache()

    ids = {row["id"] for row in df_silver.select("id").collect()}
    assert ids == {1, 2}

    expected_types = {
        "id":                 "bigint",
        "value":              "double",
        "event_time":         "timestamp",
        "partition_date":     "date"
    }
    actual_types = {f.name: f.dataType.simpleString() for f in df_silver.schema.fields}
    for col_name, dtype in expected_types.items():
        assert actual_types[col_name] == dtype, f"Coluna {col_name} deveria ser {dtype}, mas é {actual_types[col_name]}"

    row1 = df_silver.filter(col("id") == 1).collect()[0]
    assert abs(row1["value"] - 10.5) < 1e-6
    assert row1["partition_date"] == date(2025, 6, 1)


def test_prepare_silver_df_empty_when_no_data_in_range():
    data = [
        {"id": 5, "value": "5.0", "event_time": "2025-05-01 10:00:00"},
        {"id": 6, "value": "6.0", "event_time": "2025-08-15 15:00:00"},
    ]
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("value", StringType(), True),
        StructField("event_time", StringType(), True),
    ])
    df_bronze = spark.createDataFrame(data, schema)

    start_date = date(2025, 6, 1)
    end_date   = date(2025, 7, 31)

    df_silver = prepare_silver_df(spark, df_bronze, start_date, end_date)
    assert df_silver.rdd.isEmpty()
