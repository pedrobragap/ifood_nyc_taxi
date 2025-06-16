from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, to_date, month, year, lit
from pyspark.sql.types import LongType, DoubleType, TimestampType, IntegerType
from datetime import date
from .config import CASTS, RENAME, SILVER_COLUMNS


def prepare_silver_df(self, df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    filtered = (
        df
        .filter(
            (col("tpep_pickup_datetime").between(start_date, end_date)) &
            (col("total_amount") > 0) &
            (col("passenger_count") > 0)
        )
    )

    df = filtered.select(*SILVER_COLUMNS).dropDuplicates()
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