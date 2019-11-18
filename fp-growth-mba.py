from pyspark.mllib.fpm import FPGrowth
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import unix_timestamp, from_unixtime
#import plotly.graph_objs as go
#from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import shutil
#init_notebook_mode(connected=True)
spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
df = sqlContext.read.load('/test_dev/mba-code/dataset.csv', format='csv', header='true', inferSchema='true')
df.select("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate","InvoiceDateWS" ,date_format(from_unixtime(unix_timestamp('InvoiceDateWS', 'mm/dd/yyy')), 'EEEE').alias('weekday'),"CustomerID","Country").write.save("Invoices.parquet", format="parquet")
parquetFile = spark.read.parquet("Invoices.parquet")
# Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
DescriptionGrp = spark.sql("SELECT distinct InvoiceNo,StockCode FROM parquetFile group by InvoiceNo,StockCode")
print(DescriptionGrp.rdd.take(2))
minSupport = 0.05 * DescriptionGrp.rdd.count()
apr_tem=DescriptionGrp.rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x + y)
transactions = apr_tem.map(lambda x: (x[1]))
model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)