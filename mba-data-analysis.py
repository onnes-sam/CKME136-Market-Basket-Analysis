import sys
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName("Cloudera")
sc = SparkContext(conf=conf)
#mba_df = sc.textFile("hdfs:////user/root/capstone/dataset/dataset.csv")
#hdfs://quickstart.cloudera:8020/user/cloudera/
mba_df = sc.textFile("hdfs://sandbox.hortonworks.com:4040/user/root/capstone/dataset/dataset.csv")
print(mba_df.collect())

#from pyspark.sql import SparkSession
#spark = SparkSession.builder.getOrCreate()
#mba_df = spark.read\
#  .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')\
#  .option('header', 'true')\
#  .load('hdfs://sandbox.hortonworks.com:8020/user/root/capstone/dataset/dataset.csv')