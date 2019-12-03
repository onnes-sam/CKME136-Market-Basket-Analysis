import sys, os, time, datetime
from operator import add
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp, from_unixtime
import shutil
import pyarrow
from pyspark.sql.functions import collect_list, col
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
import pandas as pd
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
for row in transactions.collect():
	for pair in itertools.combinations(row,2):
		transaction_set.append(pair)