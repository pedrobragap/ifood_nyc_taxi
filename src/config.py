from pyspark.sql.types import LongType, DoubleType, TimestampType, IntegerType

CASTS = {
    "vendor_id": LongType(),        
    "passenger_count": LongType(),
    "total_amount": DoubleType(),  
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
}

RENAME = {
    "VendorID": "vendor_id",        
    "lpep_pickup_datetime": "tpep_pickup_datetime",
    "lpep_dropoff_datetime": "tpep_dropoff_datetime"
}

DATA_CONFIG = {
    "base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data",
    "taxi_colors": ["yellow", "green"],
    "landing_path": "/Volumes/workspace/default/landing/nyc_taxi_trip_records"
}

DB_CONFIG = {
    "bronze_schema": "bronze",
    "bronze_table": "nyc_taxi_trip_records",
    "silver_schema": "silver",
    "silver_table": "nyc_taxi_trip_records"
}
