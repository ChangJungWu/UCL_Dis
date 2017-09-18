#import time
#start_time = time.time

import os
import csv
import numpy as np
import pandas as pd
import datetime
import time
from multiprocessing import Pool


# Changing directory
os.chdir("A:\\UCL_Dis\\data")

# Reading Tables
df = pd.read_csv('uk_sales_1516_clean.csv')


# Deleting Unwanted Coulmns
df = df.drop(["Company code", "Company code.1", "Profit Center", \
"Material", "Sold-to party.1_x", "Strategic Solution", \
"Sold-to party.1_y", "Profit Center","Group key (MD).1", \
"Material.1", "Mfr Part Number (MD)", \
"Group key (MD)"], 1)


# Delete orders less than 4
df = df.groupby('Sold-to party').filter(lambda x: len(x) >=4)


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

#churnPeriod = datetime.datetime.fromtimestamp(30)
churnPeriod = 30



pass
# Creating new list to contain unique firm's id
####################### DF SHOULD BE CHANGED
df = df.loc[df['Sold-to party'].isin(['587267', '633517', '361728', '682658', '711033', '694296'])]
#df = df.loc[df['Sold-to party'].isin(['682658'])]
#df = df.loc[df['Sold-to party'].isin(['587267'])]
#idUniqueList = list(df['Sold-to party'].unique())[1]
idUniqueList = ['587267', '633517', '361728', '682658', '711033', '694296']
#idUniqueList = ['682658']
#idUniqueList = ['587267']
pass


# Select only party and time
timeDF = df[['Sold-to party', 'Calendar day']]
idUniqueList = list(sorted(set(df['Sold-to party'])))
del df



# This should not change
#timeDF['DatasetLastDate'] = df['Calendar day'].max()
timeDF['DatasetLastDate'] = lastDayofDataset

# These are default settings, so sould be changed in the for loop
timeDF['StartEventDate'] = timeDF['Calendar day'].min()
timeDF['NextTransactionPeriod'] = 0
timeDF['CHR'] = 0
timeDF['ING'] = 0
timeDF['ATC'] = 0

TimeMapDF = pd.DataFrame()    



def churnGenerate(partyName):
    global TimeMapDF
    global lastDayofDataset
    ndf = timeDF[timeDF['Sold-to party'] == partyName] # Will update through loops
    
    churnPointList = []
    aboutToChurnList = []
    ingList = []
    
    lastEventDay = ndf['Calendar day'][ndf.index[-1]]
    lastToEndDay = (lastDayofDataset-lastEventDay).days
    
    ndf['NextTransactionPeriod'][ndf.index[-1]] = lastToEndDay
    ndf['StartEventDate'] = ndf['Calendar day'].min()
    

        # This is for finding Churn Point (Changing Point) 
        # In this loop, we will first calculate the period between 
            # this datapoint and the newxt data point
        # Also, we will find the change point from the time period
    if lastToEndDay > churnPeriod:
        ndf['NextTransactionPeriod'][ndf.index[-1]] = lastToEndDay
        churnPointList.append(ndf['Calendar day'][ndf.index[-1]])
            
            
    for row in range(ndf.shape[0]-1, -1, -1):     

        ndf.loc[ndf.index[row-1], 'NextTransactionPeriod'] = \
        (ndf.loc[ndf.index[row], 'Calendar day']-\
         ndf.loc[ndf.index[row-1],'Calendar day']).days         

            # For the same day transaction
        if ndf.loc[ndf.index[row-1], 'Calendar day'] == ndf.loc[ndf.index[row], 'Calendar day']:
            ndf.loc[ndf.index[row-1], 'NextTransactionPeriod'] = \
            ndf.loc[ndf.index[row], 'NextTransactionPeriod']
            
            
        if ndf['NextTransactionPeriod'][ndf.index[row]]>churnPeriod:
            churnPointList.append(ndf['Calendar day'][ndf.index[row]])

        
    ndf['NextTransactionPeriod'][ndf.index[-1]] = lastToEndDay
    #print("churnPointList is %s" % churnPointList)
        
    # Remove duplicates of churnPointList
    churnPointList = list(sorted(set(churnPointList)))

        
    # Assigning Values to 'CHR', 'ATC', and 'ING'
    ndf.loc[ndf['Calendar day'].isin(churnPointList), 'CHR'] = 1
    ndf.loc[ndf['Calendar day'].isin(churnPointList), 'ATC'] = 0
    ndf.loc[ndf['Calendar day'].isin(churnPointList), 'ING'] = 0

        
    # This is for finding About To Churn data point        
    for i in range(len(churnPointList)):
                            
        mask = (ndf['Calendar day'] < pd.Timestamp(churnPointList[i]) ) &\
        (ndf['Calendar day']>=pd.Timestamp(churnPointList[i])-pd.Timedelta(days=churnPeriod))
                                  
        aboutToChurnList += ndf.loc[mask, 'Calendar day'].tolist()

            
        # Assigning Values to 'CHR', 'ATC', and 'ING'
        for e in aboutToChurnList:
            ndf.loc[ndf['Calendar day'] == e, 'ATC'] = 1
            ndf.loc[ndf['Calendar day'] == e, 'CHR'] = 0
            ndf.loc[ndf['Calendar day'] == e, 'ING'] = 0    
            
                       
    # Remove duplicates of aboutToChurnList
    aboutToChurnList = list(sorted(set(aboutToChurnList))) 
            
        
        # This is for finding ING data point       
        # Setting the condition for ING
            # Since if 2 not in []:
            #          print("True")
            # still works, we don't need to set the exception
    for x in range(len(ndf['Calendar day'])):
        if ndf.loc[ndf.index[x], 'Calendar day'] not in churnPointList+aboutToChurnList:
            ingList.append(ndf.loc[ndf.index[x], 'Calendar day'])
        else: continue
        

    # Remove duplicates of ingList
    ingList = list(sorted(set(ingList)) )

        
    # Assigning Values to 'CHR', 'ATC', and 'ING'
    ndf.loc[ndf['Calendar day'].isin(ingList), 'ING'] = 1
    ndf.loc[ndf['Calendar day'].isin(ingList), 'ATC'] = 0
    ndf.loc[ndf['Calendar day'].isin(ingList), 'CHR'] = 0
    
    
    TimeMapDF = pd.concat([TimeMapDF, ndf], ignore_index=True)                
    return TimeMapDF
        
    pass # CAN BE DELETED
    #print("Sold-to party is %s" % partyName)
    #print("churnPointList is %s" % churnPointList)
    #print("aboutToChurnList is %s" % aboutToChurnList)
    #print("ingList is %s" % ingList)
    #print("=====================================================")
    pass
        
        
    # Make as default
    ndf = pd.DataFrame()
    churnPointList = []
    aboutToChurnList = []
    ingList = []
    lastEventDay = 0
    lastToEndDay = 0

	
	
from contextlib import closing


if __name__ == "__main__":
	with closing(Pool(processes=4)) as pool:
		ret_list = pool.map(churnGenerate, idUniqueList)
		pd.concat(ret_list)
		pool.terminate()
	
	#p = Pool(processes=4)
	#p.close()
	#extractor = parallelTestModule.ParallelExtractor()
	#extractor.runInParallel(numProcesses=4)


#with closing(Pool(processes=4)) as pool:
	#freeze_support()
	#pool.map(churnGenerate, idUniqueList)
	#pool.terminate()
	
	
	

# if __name__ == "__main__":
  
    # p = Pool(processes=4)
    # p.map(churnGenerate, idUniqueList) 
    # p.close()
    # p.join()
    #print("Pool took:", time.time()-start_time)


# def applyParallel(timeDF, churnGenerate):
	# with Pool(processes=4) as p:
		# ret_list = p.map(churnGenerate, idUniqueList)
		# p.close()
		# return pd.concat(ret_list)	
	
# if __name__ == '__main__':
	# applyParallel(timeDF, churnGenerate)
	#p.close()
	
	
print(TimeMapDF)

TimeMapDF.to_csv('TimeMapDF_LoopThroughIDList_Pool.csv', sep=',')

#print("---%s seconds---" % time.time()-start_time)




















































