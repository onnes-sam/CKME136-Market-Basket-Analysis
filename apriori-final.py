import sys, os, time, datetime
from operator import add
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp, from_unixtime
import shutil
from apyori import apriori, load_transactions
from pyspark.sql.functions import collect_list, col
import pandas as pd
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
print(transactions.take(5))
#transactionsrdd = transactions.map(lambda x: x[1])
transactionrecord = load_transactions(transactionsrdd)
print (transactionrecord)
association_rules_mba=apriori(transactionrecord,min_support=0.002,min_confidence=0.002,min_lift=1.2,min_length=2)
transactionsparaRDD = sc.parallelize(association_rules_mba)
rulesDF = sqlContext.createDataFrame(transactionsparaRDD)
ptint(rulesDF)