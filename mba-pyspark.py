from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
mba_df = spark.read\
  .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')\
  .option('header', 'true')\
  .load('dataset.csv')