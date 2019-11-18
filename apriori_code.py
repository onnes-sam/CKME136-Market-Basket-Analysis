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
from operator import add
from apyori import apriori, load_transactions
import pyspark.sql.functions as f
from mlxtend.preprocessing import TransactionEncoder
from pyspark.sql.functions import explode
DescriptionGrp = spark.sql("SELECT distinct InvoiceNo,StockCode FROM parquetFile group by InvoiceNo,StockCode")
print(DescriptionGrp.rdd.take(2))
minSupport = 0.05 * DescriptionGrp.rdd.count()
apr_tem=DescriptionGrp.rdd.map(lambda x: (x[0], [x[1]]).reduceByKey(lambda x,y: x + y))
#print(apr_tem)
transactions_mba=apr_tem.map(lambda x: (x[1]))
association_rules_mba=apriori(transactions_mba,min_support=0.5,min_confidence=0.7,min_lift=1.2,min_length=2)