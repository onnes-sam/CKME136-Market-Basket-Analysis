import sys, os, time, datetime
from operator import add
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp, from_unixtime
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
import itertools
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col
import pandas as pd
import csv
csv_file = "/tmp/transactionrecord.csv"
spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
sc = spark.sparkContext
sqlContext = HiveContext(sc)
df = sc.textFile('hdfs:/user/capstone/dataset.csv').map(lambda line: line.split(","))
header = df.first()
fields = [StructField(field_name, StringType(), True) for field_name in header] #get the types of header variable fields
schema = StructType(fields)
filter_data = df.filter(lambda row:row != header)
SelectDf = sqlContext.createDataFrame(filter_data, schema=schema)
SelectDf.registerTempTable("transactions")
DescriptionGrp = sqlContext.sql("SELECT distinct InvoiceNo,Description FROM transactions group by InvoiceNo,Description")
transactions = DescriptionGrp.groupBy("InvoiceNo").agg(collect_list("Description").alias("desc")).rdd.map(lambda x: x.desc)
transaction_set = set()
encoding = "utf-8"
on_error = "replace"
csv_columns = ['sno','tr1','tr2']
transaction_set = set()
for row in transactions.collect():
   for pair in itertools.combinations(row,2):
      transaction_set.add(pair)
outF = open("/tmp/dict123.txt", "w")
for row in transaction_set:
   line = ','.join(unicode(x).encode('utf-8') for x in row)
   #line.encode(encoding='UTF-8',errors='replace')
   print >> outF, line
outF.close()
#with open("/tmp/dict123.txt", "w") as text_file:
#   for row in transaction_set:
#      line = ','.join(str(x).encode('ascii', 'ignore').decode('ascii') for x in row)
#      line.encode(encoding='UTF-8',errors='replace')
#      print(line)
#      text_file.write(line + '\n')
#with open('/tmp/dict123.csv', 'w') as csv_file:
#   writer = csv.writer(csv_file)
#   for row in transaction_set:
#      print(row)
#      writer.writerow(row)
#transactionObj = pd.DataFrame(transaction_set, columns = csv_columns)
#transactionObj.to_csv('/tmp/transactionrecord1.csv', sep=',', encoding='utf-8')