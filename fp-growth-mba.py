import sys, os, time, datetime

sparkhome = '/usr/hdp/2.6.5.0-292/spark2/'
os.environ['SPARK_HOME'] = sparkhome
os.environ['PYSPARK_PYTHON']="/usr/hdp/2.6.5.0-292/spark2/python"
sys.path.append(sparkhome + '/python')
sys.path.append(sparkhome + '/python/lib/py4j-0.10.6-src.zip')
sys.path.append(sparkhome + '/python/lib/pyspark.zip')

from pyspark.mllib.fpm import FPGrowth
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.functions import concat_ws


import shutil
#init_notebook_mode(connected=True)
spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
sc = spark.sparkContext
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
schema = StructType([StructField("id", StringType(), True),StructField("items", ArrayType(StringType()), True)])
transactions = spark.createDataFrame(apr_tem, schema)
print(transactions.show(2))
##transactions_fp=apr_tem.map(lambda x: (x[1]))
#print(transactions_fp.take(2))
#schema = StructType([StructField("test_123",ArrayType(StringType(),True),True)])
#fields = [StructField(field_name, StringType(), True) for field_name in schema.split(',')]
#schema = StructType(fields)
##final_transactions_rdd = sc.parallelize(transactions_fp.collect())
##final_transactions = final_transactions_rdd.map(lambda x : ','.join(x))
##print(final_transactions.take(2))
#transactions = spark.createDataFrame([final_transactions])
##transactions = final_transactions.map(lambda line: line.strip().split(','))
##print(transactions.take(2))
fpgrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
##fpgrowth = FPGrowth(minSupport=0.5, minConfidence=0.6)
model = fpgrowth.fit(transactions)
# Display frequent itemsets.
model.freqItemsets.show()
# Display generated association rules.
model.associationRules.show()
# transform examines the input items against all the association rules and summarize the
# consequents as prediction
model.transform(df).show()