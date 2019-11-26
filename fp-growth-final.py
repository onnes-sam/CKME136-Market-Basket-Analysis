import sys, os, time, datetime
from pyspark.mllib.fpm import FPGrowth
from operator import add
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp, from_unixtime
import shutil
from pyspark.sql.functions import collect_list, col
#init_notebook_mode(connected=True)
#spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
#sc = spark.sparkContext
#sc = SparkContext("local", "Simple App")
sqlContext = HiveContext(sc)
#df = sqlContext.read.load(source='com.databricks.spark.csv', path = 'hdfs:/user/cloudera/capstone/dataset.csv', format='csv', header='true', inferSchema='true',encoding='UTF-8')
#df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('file.csv')
df = sc.textFile('hdfs:/user/cloudera/capstone/dataset.csv').map(lambda line: line.split(","))
fields = [StructField(field_name, StringType(), True) for field_name in header] #get the types of header variable fields
schema = StructType(fields)
filter_data = df.filter(lambda row:row != header)
SelectDf = sqlContext.createDataFrame(filter_data, schema=schema)
SelectDf.registerTempTable("transactions")
DescriptionGrp = sqlContext.sql("SELECT distinct InvoiceNo,Description FROM transactions group by InvoiceNo,Description")
DescriptionGrp.show(5)
#df.select("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate","CustomerID","Country").write.save("hdfs:/user/cloudera/capstone/Invoices.parquet", format="parquet")
#DescriptionGrp = sqlContext.sql("SELECT distinct InvoiceNo,Description FROM parquet.`hdfs:/user/cloudera/capstone/Invoices.parquet` group by InvoiceNo,Description")
transactions = DescriptionGrp.groupBy("InvoiceNo").agg(collect_list("Description").alias("desc")).rdd.map(lambda x: x.desc)
transactionsrdd = sc.parallelize(transactions.collect(),10000)
model = FPGrowth.train(transactionsrdd, 0.002, 10)
result = model.freqItemsets()
modelresultsDF = sqlContext.createDataFrame(result)
modelresultsDF.registerTempTable("modelresults")
MostOrdered = sqlContext.sql('select items, freq from modelresults where size(items) > 2 order by freq desc limit 20')
#parquetFile = spark.read.parquet("Invoices.parquet")
# Parquet files can also be used to create a temporary view and then used in SQL statements.
#parquetFile.createOrReplaceTempView("parquetFile")
#DescriptionGrp = sqlContext.sql("SELECT distinct InvoiceNo,StockCode FROM parquetFile group by InvoiceNo,StockCode")
#print(DescriptionGrp.rdd.take(2))
#minSupport = 0.05 * DescriptionGrp.rdd.count()
#apr_tem=DescriptionGrp.rdd.map(lambda x: (x[0], list([x[1]]))).reduceByKey(lambda x,y: x + y)
#final_transactions = apr_tem.map(lambda x: (x[1]))
#schema = StructType([StructField("test_123",ArrayType(StringType(),True),True)])
#transactions_dataframe = sqlContext.createDataFrame(final_transactions,schema)
#transactions = transactions_dataframe.map(lambda line: line.strip().split(','))
#schema = StructType([StructField("id", StringType(), True),StructField("items", ArrayType(StringType()), True)])
#transactions = spark.createDataFrame(apr_tem, schema)
#print(transactions.show(2))
model = FPGrowth.train(transactions, minSupport=0.02, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:print(fi)
