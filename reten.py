import time
start_time = time.time()

import os
import csv
import numpy as np
import pandas as pd
import datetime
from multiprocessing import Pool

# Changing Directory
os.chdir("/media/sf_share/csv")


# Reading Data
df = pd.read_csv('newdf_all_two_more.csv')

# Sort the order by day			  
df['Calendar day'] =pd.to_datetime(df['Calendar day']).sort_values() # This now sorts in date order
df = df.sort_values(by='Calendar day')


# Make firm ID number as STR
df['Sold-to party'] = df['Sold-to party'].apply(str)

#check the result
print(type(df['Sold-to party'][0]))


# Check the column is time series type
df['Calendar day'] = pd.to_datetime(df['Calendar day']); 
print(type(df['Calendar day'].iloc[0]))


print("Start date : %s" % df['Calendar day'].min())
print("End date   : %s" % df['Calendar day'].max()) 
lastDayofDataset = df['Calendar day'].max()


# Select only party and time
df = df[['Sold-to party', 'Calendar day', 'NextTransactionPeriod', 'CHR']]
idUniqueList = list(sorted(set(df['Sold-to party'])))


# Preallocation
df['Returning_2m'] = 0 #31~60
df['Returning_3m'] = 0 #61~90
df['Returning_4m'] = 0 #91~120
df['Returning_5m'] = 0 #121~150
df['Returning_6m'] = 0 #151~180
df['Returning_over_6m'] = 0 #over 6 months
df['NeverReturned'] = 0

pdf = pd.DataFrame()    



def retentionGenerate(partyName):
	ndf = df[df['Sold-to party'] == partyName] # Will update through loops
	lastDayTransaction = ndf['Calendar day'].max()
	
	for row in range(0,ndf.shape[0]):
		if ndf.loc[ndf.index[row],'Calendar day'] != lastDayTransaction: 
		# If it is not the last day transaction
			if ndf.loc[ndf.index[row],'NextTransactionPeriod'] in range(31,61):
				ndf.loc[ndf.index[row],'Returning_2m'] = 1
			elif ndf.loc[ndf.index[row],'NextTransactionPeriod'] in range(61,91):
				ndf.loc[ndf.index[row],'Returning_3m'] = 1
			elif ndf.loc[ndf.index[row],'NextTransactionPeriod'] in range(91,121):
				ndf.loc[ndf.index[row],'Returning_4m'] = 1
			elif ndf.loc[ndf.index[row],'NextTransactionPeriod'] in range(121,151):
				ndf.loc[ndf.index[row],'Returning_5m'] = 1
			elif ndf.loc[ndf.index[row],'NextTransactionPeriod'] in range(151,181):
				ndf.loc[ndf.index[row],'Returning_6m'] = 1
			elif ndf.loc[ndf.index[row],'NextTransactionPeriod'] >180:
				ndf.loc[ndf.index[row],'Returning_over_6m'] = 1
			else: continue
		else: # If it is the last transaction 
			if ndf.loc[ndf.index[row],'CHR'] ==1:
				ndf.loc[ndf.index[row],'NeverReturned'] = 1
			else: continue
		
	
	return ndf


from contextlib import closing


if __name__ == "__main__":
	with closing(Pool(processes=4)) as pool:
		ret_list = pool.map(retentionGenerate, idUniqueList)
		pdf = pdf.append(pool.map(retentionGenerate, idUniqueList))
		pool.terminate()

		
#print(pdf)
pdf.to_csv('retention_multi.csv', sep=',')
print("All Done!")
print("---%s seconds---" % (time.time()-start_time))