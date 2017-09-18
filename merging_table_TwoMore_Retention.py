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
os.chdir("/media/sf_share/csv")


# Reading Data
df = pd.read_csv('newdf_all_two_more.csv')
ndf = pd.read_csv('retention_multi.csv')


# Dop Unwanted Columns
# df = df.drop(["Company code", "Company code.1", "Profit Center", "Material", \
              # "Sold-to party.1_x", "Strategic Solution", "Sold-to party.1_y", \
              # "Profit Center","Group key (MD).1", "Material.1", "Mfr Part Number (MD)", \
              # "Group key (MD)"], 1)

df = df.drop(["Unnamed: 0"], 1)
ndf = ndf.drop(["Unnamed: 0"], 1)
 
# Delete orders less than 4
df = df.groupby('Sold-to party').filter(lambda x: len(x) >=4)


# Sort the order by day
df['Calendar day'] =pd.to_datetime(df['Calendar day']).sort_values() # This now sorts in date order
df = df.sort_values(by='Calendar day')

ndf['Calendar day'] = pd.to_datetime(ndf['Calendar day']).sort_values() # This now sorts in date order
ndf = ndf.sort_values(by='Calendar day')

# Make firm ID number as STR
df['Sold-to party'] = df['Sold-to party'].apply(str)
ndf['Sold-to party'] = ndf['Sold-to party'].apply(str)

#check the result
print(type(df['Sold-to party'][0]))
print(type(ndf['Sold-to party'][0]))


# Check the column is time series type
df['Calendar day'] = pd.to_datetime(df['Calendar day']);
ndf['Calendar day'] = pd.to_datetime(ndf['Calendar day']); 

print(type(df['Calendar day'].iloc[0]))
print(type(ndf['Calendar day'].iloc[0]))


# Sort the dataframe by Sold-to party adn calendar day
df = df.sort_values(['Sold-to party', 'Calendar day'], ascending=[True, True]).reset_index(drop=True)
ndf = ndf.sort_values(['Sold-to party', 'Calendar day'], ascending=[True, True]).reset_index(drop=True)


cols_to_use = ndf.columns.difference(df.columns)
print(cols_to_use)
newdf = pd.concat([df, ndf[cols_to_use]], axis=1)
print(newdf.head())


newdf.to_csv('newdf_all_retention.csv', sep=',')