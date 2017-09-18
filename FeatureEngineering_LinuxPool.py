import time
start_time = time.time()


import os
import csv
import numpy as np
import pandas as pd
import datetime
from multiprocessing import Pool


# Changing directory
os.chdir("/media/sf_share")

# Reading DataFrame
df = pd.read_csv('TimeMapDF_LoopThroughIDList_Pool.csv')


pdf = pd.DataFrame()  


# Make firm ID number as STR
df['Sold-to party'] = df['Sold-to party'].apply(str)

#check the result
print(type(df['Sold-to party'][0]))


# Check the column is time series type
df['Calendar day'] = pd.to_datetime(df['Calendar day']); 
df['DatasetLastDate'] = pd.to_datetime(df['DatasetLastDate']); 
df['StartEventDate'] = pd.to_datetime(df['StartEventDate']); 

print(type(df['Calendar day'].iloc[0]))
print(type(df['DatasetLastDate'].iloc[0]))
print(type(df['StartEventDate'].iloc[0]))


# Creating new list to contain unique firm's id
idUniqueList = list(df['Sold-to party'].unique())


# Preallocation
df['PreviousTransactionPeriod'] = 0
df['LargestTimeInterval'] = 0
df['SmallestTimeInterval'] = 'First Date'
df['DaysSinceFirstEvent'] = 0
df['NumbersOfEventsSinceStart'] = 1



def timeInterval(partyName):
    
    global df
    startEventDay = df['Calendar day'].min()
    ndf = df[df['Sold-to party'] == partyName] # Will update through loops

    calendarList = ndf['Calendar day'].tolist()
    
    
    
    for row in range(1,ndf.shape[0]):
        
        ## PreviousTransactionPeriod
        ndf.loc[ndf.index[row], 'PreviousTransactionPeriod'] = \
        (ndf.loc[ndf.index[row], 'Calendar day']-\
         ndf.loc[ndf.index[row-1], 'Calendar day']).days
        
        # For the same day transaction
        if ndf.loc[ndf.index[row-1], 'Calendar day'] == ndf.loc[ndf.index[row], 'Calendar day']:
            ndf.loc[ndf.index[row], 'PreviousTransactionPeriod'] = \
            ndf.loc[ndf.index[row-1], 'PreviousTransactionPeriod']
    
    
    
        ## Numbers of days since the first event
        ndf.loc[ndf.index[row], 'DaysSinceFirstEvent'] = \
        (ndf.loc[ndf.index[row], 'Calendar day']-ndf.loc[ndf.index[row],'StartEventDate']).days
        
        ## Numbers of Event in the last T days
        ndf.loc[ndf.index[row], 'NumbersOfEventsSinceStart'] = \
        ndf.loc[:ndf.index[row],:].shape[0]            
    
        
        
    for row in range(ndf.shape[0]):
        
        ## Largest Time Internval between two events
        ndf.loc[ndf.index[row], 'LargestTimeInterval'] = \
        ndf.loc[:ndf.index[row], 'PreviousTransactionPeriod'].max() 
        
        ## Smallest Time Internval between two events
        ndf.loc[ndf.index[row], 'SmallestTimeInterval'] = \
        ndf[ndf['PreviousTransactionPeriod']>0].loc[:ndf.index[row],'PreviousTransactionPeriod'].min()
        
   
    ndf = ndf.astype(object).fillna('First Date')
    return ndf



# total = 0
# for partyName in idUniqueList:
    # pdf = pdf.append(timeInterval(partyName))
	# print("%s / 7200" %total)
	
from contextlib import closing


if __name__ == "__main__":

    pool = Pool()
    pdf = pdf.append(pool.map(timeInterval, idUniqueList))
    pool.close()
    pool.join()
	
pdf.to_csv('pdf_LoopThroughIDList_Pool.csv', sep=',')
print("---%s seconds---" % (time.time()-start_time))















