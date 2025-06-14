import unittest
import yaml
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType, StringType
from pyspark.sql.functions import col, year, month

class TestTaxiPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # No Databricks, spark já está disponível globalmente
        cls.spark = spark
        
        # Carregar configurações do YAML
        with open('elt_nyc_taxi.yaml', 'r') as file:
            cls.config = yaml.safe_load(file)
        
        # Criar schema de teste
        cls.schema = StructType([
            StructField("VendorID", LongType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("taxi_color", StringType(), True)
        ])
        
        # Usar dados de teste do YAML
        cls.test_data = cls.config['tests']['test_data']
        cls.sample_df = cls.spark.createDataFrame(cls.test_data, cls.schema)

    def test_bronze_layer_schema(self):
        """Test if bronze layer maintains correct schema"""
        # Write to bronze table
        bronze_table = f"{self.config['layers']['bronze']['schema']}.{self.config['layers']['bronze']['table']}"
        self.sample_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        
        # Read from bronze table
        bronze_df = self.spark.read.table(bronze_table)
        
        # Verify schema
        for column in self.config['layers']['silver']['columns']:
            self.assertIn(column, bronze_df.columns)

    def test_silver_layer_filters(self):
        """Test if silver layer applies correct filters"""
        # Write to bronze table
        bronze_table = f"{self.config['layers']['bronze']['schema']}.{self.config['layers']['bronze']['table']}"
        self.sample_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        
        # Apply silver layer transformations
        silver_df = (self.spark.read.table(bronze_table)
                    .filter((year(col('tpep_pickup_datetime')) == self.config['layers']['silver']['filters']['year']) &
                           (month(col('tpep_pickup_datetime')).between(
                               self.config['layers']['silver']['filters']['start_month'],
                               self.config['layers']['silver']['filters']['end_month']
                           )) &
                           (col('total_amount') > self.config['layers']['silver']['filters']['min_amount']) &
                           (col('passenger_count') > self.config['layers']['silver']['filters']['min_passengers'])))
        
        # Write to silver table
        silver_table = f"{self.config['layers']['silver']['schema']}.{self.config['layers']['silver']['table']}"
        silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        
        # Read from silver table
        result_df = self.spark.read.table(silver_table)
        
        # Verify filters
        self.assertEqual(result_df.filter(col("total_amount") <= 0).count(), 0)
        self.assertEqual(result_df.filter(col("passenger_count") <= 0).count(), 0)
        self.assertEqual(result_df.filter(year(col("tpep_pickup_datetime")) != self.config['layers']['silver']['filters']['year']).count(), 0)
        self.assertEqual(result_df.filter(~month(col("tpep_pickup_datetime")).between(
            self.config['layers']['silver']['filters']['start_month'],
            self.config['layers']['silver']['filters']['end_month']
        )).count(), 0)

    def test_silver_layer_columns(self):
        """Test if silver layer has correct columns"""
        # Write to bronze table
        bronze_table = f"{self.config['layers']['bronze']['schema']}.{self.config['layers']['bronze']['table']}"
        self.sample_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        
        # Apply silver layer transformations
        silver_df = (self.spark.read.table(bronze_table)
                    .select(*self.config['layers']['silver']['columns']))
        
        # Write to silver table
        silver_table = f"{self.config['layers']['silver']['schema']}.{self.config['layers']['silver']['table']}"
        silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        
        # Read from silver table
        result_df = self.spark.read.table(silver_table)
        
        # Verify columns
        self.assertEqual(set(result_df.columns), set(self.config['layers']['silver']['columns']))

    def test_duplicate_removal(self):
        """Test if silver layer removes duplicates"""
        # Create duplicate data
        duplicate_data = self.sample_df.union(self.sample_df)
        
        # Write to bronze table
        bronze_table = f"{self.config['layers']['bronze']['schema']}.{self.config['layers']['bronze']['table']}"
        duplicate_data.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        
        # Apply silver layer transformations with duplicate removal
        silver_df = (self.spark.read.table(bronze_table)
                    .dropDuplicates())
        
        # Write to silver table
        silver_table = f"{self.config['layers']['silver']['schema']}.{self.config['layers']['silver']['table']}"
        silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        
        # Read from silver table
        result_df = self.spark.read.table(silver_table)
        
        # Verify no duplicates
        self.assertEqual(result_df.count(), self.sample_df.count())

# Para rodar os testes no Databricks, você pode usar:
if __name__ == '__main__':
    unittest.main() 