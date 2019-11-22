import sys, os, time, datetime
from pyspark.mllib.fpm import FPGrowth
from operator import add
#from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp, from_unixtime
import shutil
#init_notebook_mode(connected=True)
#spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
#sc = spark.sparkContext
sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)
df = sqlContext.read.load('/test_dev/mba-code/dataset.csv', format='csv', header='true', inferSchema='true',encoding='UTF-8')
df.select("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate","InvoiceDateWS" ,date_format(from_unixtime(unix_timestamp('InvoiceDateWS', 'mm/dd/yyy')), 'EEEE').alias('weekday'),"CustomerID","Country").write.save("Invoices.parquet", format="parquet")
parquetFile = spark.read.parquet("Invoices.parquet")
# Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
DescriptionGrp = spark.sql("SELECT distinct InvoiceNo,StockCode FROM parquetFile group by InvoiceNo,StockCode")
#print(DescriptionGrp.rdd.take(2))
minSupport = 0.05 * DescriptionGrp.rdd.count()
apr_tem=DescriptionGrp.rdd.map(lambda x: (x[0], list([x[1]]))).reduceByKey(lambda x,y: x + y)
transactions = apr_tem.map(lambda x: (x[1]))
#schema = StructType([StructField("id", StringType(), True),StructField("items", ArrayType(StringType()), True)])
#transactions = spark.createDataFrame(apr_tem, schema)
#print(transactions.show(2))
model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)