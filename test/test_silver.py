import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, TimestampType, StringType, DateType
)
from pyspark.sql.functions import col, to_timestamp, to_date
from src.utils import prepare_silver_df

def test_prepare_silver_df_filters_and_casts(spark):
    """Test if silver layer correctly filters and casts data"""
    data = [
        {
            "vendor_id": 1,
            "passenger_count": 2,
            "tpep_pickup_datetime": "2025-06-01 09:00:00",
            "tpep_dropoff_datetime": "2025-06-01 09:30:00",
            "total_amount": 25.50,
            "taxi_color": "yellow",
            "partition_date": "2025-06-01"
        },
        {
            "vendor_id": 2,
            "passenger_count": 0,
            "tpep_pickup_datetime": "2025-07-01 12:30:00",
            "tpep_dropoff_datetime": "2025-07-01 13:00:00",
            "total_amount": 0.00,
            "taxi_color": "yellow",
            "partition_date": "2025-07-01"
        }
    ]
    
    schema = StructType([
        StructField("vendor_id", LongType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("taxi_color", StringType(), True),
        StructField("partition_date", StringType(), True)
    ])
    
    df_bronze = spark.createDataFrame(data, schema)
    
    # Cast datetime columns to timestamp
    df_bronze = df_bronze.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    df_bronze = df_bronze.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    df_bronze = df_bronze.withColumn("partition_date", to_date(col("partition_date")))
    
    start_date = "2025-06-01"
    end_date = "2025-07-31"
    
    df_silver = prepare_silver_df(spark, df_bronze, start_date, end_date)
    
    # Verify filters
    assert df_silver.count() == 1
    row = df_silver.collect()[0]
    assert row["passenger_count"] > 0
    assert row["total_amount"] > 0
    
    # Verify column types
    expected_types = {
        "vendor_id": "bigint",
        "passenger_count": "bigint",
        "total_amount": "double",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "taxi_color": "string",
        "partition_date": "date"
    }
    actual_types = {f.name: f.dataType.simpleString() for f in df_silver.schema.fields}
    for col_name, dtype in expected_types.items():
        assert actual_types[col_name] == dtype, f"Column {col_name} should be {dtype}, but is {actual_types[col_name]}"

def test_prepare_silver_df_empty_when_no_data_in_range(spark):
    """Test if silver layer returns empty DataFrame when no data in date range"""
    data = [
        {
            "vendor_id": 1,
            "passenger_count": 2,
            "tpep_pickup_datetime": "2025-06-01 09:00:00",
            "tpep_dropoff_datetime": "2025-06-01 09:30:00",
            "total_amount": 25.50,
            "taxi_color": "yellow",
            "partition_date": "2025-06-01"
        }
    ]
    
    schema = StructType([
        StructField("vendor_id", LongType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("taxi_color", StringType(), True),
        StructField("partition_date", StringType(), True)
    ])
    
    df_bronze = spark.createDataFrame(data, schema)
    
    # Cast datetime columns to timestamp
    df_bronze = df_bronze.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    df_bronze = df_bronze.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    df_bronze = df_bronze.withColumn("partition_date", to_date(col("partition_date")))
    
    start_date = "2025-07-01"
    end_date = "2025-07-31"
    
    df_silver = prepare_silver_df(spark, df_bronze, start_date, end_date)
    assert df_silver.count() == 0
