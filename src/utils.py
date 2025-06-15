from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, to_date, month, year, lit
from pyspark.sql.types import LongType, DoubleType, TimestampType, IntegerType
from datetime import date


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

RENAME = {
    "VendorID": "vendor_id",        
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",    
    "DOLocationID": "do_location_id",
    "lpep_pickup_datetime":"tpep_pickup_datetime",
    "lpep_dropoff_datetime": "tpep_dropoff_datetime"
}

def prepare_silver_df(self,df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    filtered = (
        df
        .filter(
            (col("tpep_pickup_datetime").between(start_date, end_date)) &
            (col("total_amount") > 0) &
            (col("passenger_count") > 0)
        )
    )
    selected_cols = [
        "vendor_id",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "taxi_color",
        "partition_date"
    ]

    df = filtered.select(*selected_cols).dropDuplicates()

    return df 

def month_range(start: date, end: date):
    cur = start.replace(day=1)
    while cur <= end:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)


def prepare_bronze_df(df: DataFrame, taxi_color: str) -> DataFrame:
    rename_exprs = {c: nc for c, nc in RENAME.items() if c in df.columns}
    df = df.withColumnsRenamed(rename_exprs)

    cast_exprs = {c: col(c).cast(t) for c, t in CASTS.items() if c in df.columns}
    df = df.withColumns(cast_exprs)

    if "Airport_fee" in df.columns:
        df = (
            df
            .withColumn("airport_fee", coalesce(col("airport_fee"), col("Airport_fee")))
            .drop("Airport_fee")
        )

    select_cols = [c for c in df.columns if c in CASTS]
    df = df.select(*select_cols)

    return df.withColumns({
        "partition_date": to_date(col("tpep_pickup_datetime")),
        "taxi_color":      lit(taxi_color)
    })

def write_delta(self, df: DataFrame, schema: str, table: str):
    
    self.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("mergeSchema", "true")
          .partitionBy("partition_date")
          .saveAsTable(f"{schema}.{table}")
    )