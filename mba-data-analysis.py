import pandas as pd
from datetime import datetime
retail_dtypes = {'InvoiceNo': 'int', 'StockCode': 'str', 'Description': 'str', 'Quantity': 'int', 'InvoiceDate': 'datetime', 'UnitPrice': 'float', 'CustomerID': 'int', 'Country': 'str'}
file=pd.read_csv('dataset.csv', dtype=retail_dtypes, parse_dates=True)
print(file)
