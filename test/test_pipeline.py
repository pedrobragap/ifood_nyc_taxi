import unittest
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType, StringType
from pyspark.sql.functions import col, year, month

class TestTaxiPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # No Databricks, spark já está disponível globalmente
        cls.spark = spark
        
        # Criar schema de teste
        cls.schema = StructType([
            StructField("VendorID", LongType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("taxi_color", StringType(), True)
        ])
        
        # Criar dados de teste
        cls.test_data = [
            (1, 2, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 25.0, "yellow"),
            (2, 1, "2023-01-02 11:00:00", "2023-01-02 11:15:00", 15.0, "green"),
            (1, 3, "2023-01-03 12:00:00", "2023-01-03 12:45:00", 0.0, "yellow"),  # Invalid total_amount
            (2, 0, "2023-01-04 13:00:00", "2023-01-04 13:20:00", 20.0, "green"),  # Invalid passenger_count
        ]
        
        cls.sample_df = cls.spark.createDataFrame(cls.test_data, cls.schema)

    def test_bronze_layer_schema(self):
        """Test if bronze layer maintains correct schema"""
        # Write to bronze table
        self.sample_df.write.format("delta").mode("overwrite").saveAsTable("bronze.nyc_taxi_trip_records")
        
        # Read from bronze table
        bronze_df = self.spark.read.table("bronze.nyc_taxi_trip_records")
        
        # Verify schema
        self.assertIn("VendorID", bronze_df.columns)
        self.assertIn("passenger_count", bronze_df.columns)
        self.assertIn("tpep_pickup_datetime", bronze_df.columns)
        self.assertIn("tpep_dropoff_datetime", bronze_df.columns)
        self.assertIn("total_amount", bronze_df.columns)
        self.assertIn("taxi_color", bronze_df.columns)
        self.assertIn("year", bronze_df.columns)
        self.assertIn("month", bronze_df.columns)

    def test_silver_layer_filters(self):
        """Test if silver layer applies correct filters"""
        # Write to bronze table
        self.sample_df.write.format("delta").mode("overwrite").saveAsTable("bronze.nyc_taxi_trip_records")
        
        # Apply silver layer transformations
        silver_df = (self.spark.read.table("bronze.nyc_taxi_trip_records")
                    .filter((year(col('tpep_pickup_datetime')) == 2023) &
                           (month(col('tpep_pickup_datetime')).between(1, 5)) &
                           (col('total_amount') > 0) &
                           (col('passenger_count') > 0)))
        
        # Write to silver table
        silver_df.write.format("delta").mode("overwrite").saveAsTable("silver.nyc_taxi_trip_records")
        
        # Read from silver table
        result_df = self.spark.read.table("silver.nyc_taxi_trip_records")
        
        # Verify filters
        self.assertEqual(result_df.filter(col("total_amount") <= 0).count(), 0)
        self.assertEqual(result_df.filter(col("passenger_count") <= 0).count(), 0)
        self.assertEqual(result_df.filter(year(col("tpep_pickup_datetime")) != 2023).count(), 0)
        self.assertEqual(result_df.filter(~month(col("tpep_pickup_datetime")).between(1, 5)).count(), 0)

    def test_silver_layer_columns(self):
        """Test if silver layer has correct columns"""
        # Write to bronze table
        self.sample_df.write.format("delta").mode("overwrite").saveAsTable("bronze.nyc_taxi_trip_records")
        
        # Apply silver layer transformations
        silver_df = (self.spark.read.table("bronze.nyc_taxi_trip_records")
                    .select('VendorID', 'passenger_count', 'total_amount',
                           'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                           'taxi_color', 'year', 'month'))
        
        # Write to silver table
        silver_df.write.format("delta").mode("overwrite").saveAsTable("silver.nyc_taxi_trip_records")
        
        # Read from silver table
        result_df = self.spark.read.table("silver.nyc_taxi_trip_records")
        
        # Verify columns
        expected_columns = ['VendorID', 'passenger_count', 'total_amount',
                           'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                           'taxi_color', 'year', 'month']
        self.assertEqual(set(result_df.columns), set(expected_columns))

    def test_duplicate_removal(self):
        """Test if silver layer removes duplicates"""
        # Create duplicate data
        duplicate_data = self.sample_df.union(self.sample_df)
        
        # Write to bronze table
        duplicate_data.write.format("delta").mode("overwrite").saveAsTable("bronze.nyc_taxi_trip_records")
        
        # Apply silver layer transformations with duplicate removal
        silver_df = (self.spark.read.table("bronze.nyc_taxi_trip_records")
                    .dropDuplicates())
        
        # Write to silver table
        silver_df.write.format("delta").mode("overwrite").saveAsTable("silver.nyc_taxi_trip_records")
        
        # Read from silver table
        result_df = self.spark.read.table("silver.nyc_taxi_trip_records")
        
        # Verify no duplicates
        self.assertEqual(result_df.count(), self.sample_df.count())

# Para rodar os testes no Databricks, você pode usar:
if __name__ == '__main__':
    unittest.main() 