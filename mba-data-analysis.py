import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
retail_dtypes = {'InvoiceNo': 'str', 'StockCode': 'str', 'Description': 'str', 'Quantity': 'str', 'InvoiceDate': 'str',
                 'UnitPrice': 'str', 'CustomerID': 'str', 'Country': 'str'}
mba_dataset=pd.read_csv('dataset.csv')
pd.to_numeric(mba_dataset['UnitPrice'])
pd.to_numeric(mba_dataset['CustomerID'])
pd.to_numeric(mba_dataset['Quantity'])
pd.to_datetime(mba_dataset['InvoiceDate'], format='%m%d%Y %mm%ss', errors='ignore')
mba_dataset = mba_dataset[np.isfinite(mba_dataset['CustomerID'])]#removed rows with null customer ids
mba_dataset.CustomerID = mba_dataset.CustomerID.astype(int)
percentages = []
for gen in list(mba_dataset["Country"].unique()):
    xin = mba_dataset["InvoiceNo"][mba_dataset["Country"] == gen]
    print(xin.value_counts())
    p = round((xin.value_counts()[1] / xin.value_counts()[1].sum()) * 100, 2)
    #percentages.append(p)
    #print(gen, "(% to exit) : ", p)
#print(mba_dataset)
