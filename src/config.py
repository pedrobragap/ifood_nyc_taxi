from pyspark.sql.types import LongType, DoubleType, TimestampType, IntegerType

CASTS = {
    "vendor_id": LongType(),        
    "passenger_count": LongType(),
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "trip_distance": DoubleType(), 
    "rate_code_id": LongType(),
    "pu_location_id": LongType(),    
    "do_location_id": LongType(),
    "payment_type": LongType(),    
    "fare_amount": DoubleType(),
    "extra": DoubleType(),         
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),    
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),  
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType(),
    "trip_type": IntegerType(),
    "ehail_fee": LongType(),
}

# Column name mappings for renaming
RENAME = {
    "VendorID": "vendor_id",        
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",    
    "DOLocationID": "do_location_id",
    "lpep_pickup_datetime": "tpep_pickup_datetime",
    "lpep_dropoff_datetime": "tpep_dropoff_datetime"
}

# Data source configuration
DATA_CONFIG = {
    "base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data",
    "taxi_colors": ["yellow", "green"],
    "landing_path": "/Volumes/workspace/default/landing/nyc_taxi_trip_records"
}

# Database configuration
DB_CONFIG = {
    "bronze_schema": "bronze",
    "bronze_table": "nyc_taxi_trip_records",
    "silver_schema": "silver",
    "silver_table": "nyc_taxi_trip_records"
}

# Selected columns for silver layer
SILVER_COLUMNS = [
    "vendor_id",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "taxi_color",
    "partition_date"
] 