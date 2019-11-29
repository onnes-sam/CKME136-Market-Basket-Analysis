import sys, os, time, datetime
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp, from_unixtime
import shutil
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from pyspark.sql.functions import collect_list, col
import pandas as pd
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
print(transactions.take(5))
transactionsDF = sqlContext.createDataFrame(transactions)
print(transactionsDF.take(5))
te = TransactionEncoder()
te_ary = te.fit(transactionsDF).transform(transactionsDF)
transactionrecord = pd.DataFrame(te_ary, columns=te.columns_)
print (transactionrecord)
#association_rules_mba=apriori(transactionrecord,min_support=0.002,min_confidence=0.002,min_lift=1.2,min_length=2)
#transactionsparaRDD = sc.parallelize(association_rules_mba)
rulesDF = apriori(transactionrecord, min_support=0.6,verbose=1,low_memory=True)
ptint(rulesDF)
