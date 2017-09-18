import time
start_time = time.time()

import os
import csv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from prettytable import PrettyTable
import datetime
from multiprocessing import Pool



# Changing Directory
os.chdir("/media/sf_share")


# Reading Data
df = pd.read_csv('uk_sales_1516_clean.csv')
ndf = pd.read_csv('TimeMapDF_LoopThroughIDList_Pool.csv')
pdf = pd.read_csv('pdf_LoopThroughIDList_Pool.csv')

# Dop Unwanted Columns
# df = df.drop(["Company code", "Company code.1", "Profit Center", "Material", \
              # "Sold-to party.1_x", "Strategic Solution", "Sold-to party.1_y", \
              # "Profit Center","Group key (MD).1", "Material.1", "Mfr Part Number (MD)", \
              # "Group key (MD)"], 1)

df = df[['Sold-to party', 'Calendar day', 'Product Class. - Family (MD)', 'Super Sls Area (MD)', \
         'Global Manufacturer (MD)', 'Strategic Sublevel', 'Net Sell Price', 'Cost', \
         'Net Sales  Profit', 'Net Sales  Margin%', 'BilQtySlsUnits (Inv)']]
ndf = ndf.drop(["Unnamed: 0"], 1)
pdf = pdf.drop(['Unnamed: 0', 'Unnamed: 0.1'], 1)			  
			  

# Delete orders less than 4
df = df.groupby('Sold-to party').filter(lambda x: len(x) >=4)


# Sort the order by day
df['Calendar day'] =pd.to_datetime(df['Calendar day']).sort_values() # This now sorts in date order
df = df.sort_values(by='Calendar day')

ndf['Calendar day'] = pd.to_datetime(ndf['Calendar day']).sort_values() # This now sorts in date order
ndf = ndf.sort_values(by='Calendar day')

pdf['Calendar day'] = pd.to_datetime(pdf['Calendar day']).sort_values() # This now sorts in date order
pdf = pdf.sort_values(by='Calendar day')


# Make firm ID number as STR
df['Sold-to party'] = df['Sold-to party'].apply(str)
ndf['Sold-to party'] = ndf['Sold-to party'].apply(str)
pdf['Sold-to party'] = pdf['Sold-to party'].apply(str)

#check the result
print(type(df['Sold-to party'][0]))
print(type(ndf['Sold-to party'][0]))
print(type(pdf['Sold-to party'][0]))


# Check the column is time series type
df['Calendar day'] = pd.to_datetime(df['Calendar day']);
ndf['Calendar day'] = pd.to_datetime(ndf['Calendar day']); 
pdf['Calendar day'] = pd.to_datetime(pdf['Calendar day']); 

print(type(df['Calendar day'].iloc[0]))
print(type(ndf['Calendar day'].iloc[0]))
print(type(pdf['Calendar day'].iloc[0]))


# Sort the dataframe by Sold-to party adn calendar day
df = df.sort_values(['Sold-to party', 'Calendar day'], ascending=[True, True]).reset_index(drop=True)
ndf = ndf.sort_values(['Sold-to party', 'Calendar day'], ascending=[True, True]).reset_index(drop=True)
pdf = pdf.sort_values(['Sold-to party', 'Calendar day'], ascending=[True, True]).reset_index(drop=True)



cols_to_use = ndf.columns.difference(df.columns)
print(cols_to_use)
newdf = pd.concat([df, ndf[cols_to_use]], axis=1)
print(newdf.head())

cols_to_use = pdf.columns.difference(newdf.columns)
newdf = pd.concat([newdf, pdf[cols_to_use]], axis=1)
print(newdf.head())

newdf.to_csv('newdf_all.csv', sep=',')