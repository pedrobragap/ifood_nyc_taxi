import pytest
from datetime import date
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

from src.utils import month_range, prepare_df



def test_month_range_basic():
    start = date(2025, 1, 15)
    end   = date(2025, 3, 5)
    assert list(month_range(start, end)) == [(2025, 1), (2025, 2), (2025, 3)]

def test_prepare_df_renames_and_casts():
    data = [
        {
            "VendorID": "5",
            "passenger_count": "3",
            "lpep_pickup_datetime": "2025-06-10 08:30:00",
            "lpep_dropoff_datetime": "2025-06-10 08:50:00",
            "trip_distance": "12.5",
            "Airport_fee": "2.75",
            "total_amount": "20.00"
        }
    ]
    schema = StructType([
        StructField("VendorID", StringType(), True),
        StructField("passenger_count", StringType(), True),
        StructField("lpep_pickup_datetime", StringType(), True),
        StructField("lpep_dropoff_datetime", StringType(), True),
        StructField("trip_distance", StringType(), True),
        StructField("Airport_fee", StringType(), True),
        StructField("total_amount", StringType(), True),
    ])

    df_in = spark.createDataFrame(data, schema)
    df_out = prepare_df(df_in, taxi_color="blue")

    expected_types = {
        "vendor_id": "long",
        "passenger_count": "long",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "trip_distance": "double",
        "airport_fee": "double",
        "total_amount": "double",
        "partition_date": "date",
        "taxi_color": "string"
    }
    actual_types = {f.name: f.dataType.simpleString() for f in df_out.schema.fields}
    for col_name, dtype in expected_types.items():
        assert actual_types[col_name] == dtype

    row = df_out.collect()[0]
    assert row["vendor_id"] == 5
    assert row["passenger_count"] == 3
    assert abs(row["trip_distance"] - 12.5) < 1e-6
    assert abs(row["airport_fee"] - 2.75) < 1e-6
    assert row["taxi_color"] == "blue"
    assert row["partition_date"] == date(2025, 6, 10)
