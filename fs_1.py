'''
https://machinelearningmastery.com/feature-selection-machine-learning-python/

https://machinelearningmastery.com/feature-selection-in-python-with-scikit-learn/

'''
import time
start_time = time.time()
print("Start!")

import os
import csv
import numpy as np
import pandas as pd

# Feature Selection
from sklearn.feature_selection import mutual_info_classif
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.decomposition import PCA



# Restricting the printed floating point numbers
np.set_printoptions(precision = 4)

# Changing Directory
os.chdir("/media/sf_share/csv")

# Reading Data
df = pd.read_csv('newdf_all_combine_1.csv')
ndf = pd.read_csv('uk_sales_1516_clean.csv')


# Dop Unwanted Columns
df= df.drop([ 'Unnamed: 0', 'Unnamed: 0.1', 'StartEventDate'], 1)
ndf=ndf.drop(['Company code', 'Company code.1', 'Sold-to party.1_x', 'Sold-to party.1_y'],1)
print(df.info())
print(ndf.info())


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

# cols_to_use = ndf.columns.difference(df.columns)
# print(cols_to_use)

# newdf = pd.concat([df, ndf[cols_to_use]], axis=1)
# print(newdf.head())

# Dop Unwanted Columns
# newdf = df.drop(["NextTransactionPeriod", "DatasetLastDate", 'CHR', 'ATC', 'ING', 'Calendar day', 'Sold-to party'], 1)

newdf = df.append(ndf)
print(newdf.info())
print(newdf.head())

del df
del ndf

# Converting 'First Date'
newdf.loc[newdf.SmallestTimeInterval == 'First Date', 'SmallestTimeInterval'] = 0.0
newdf.SmallestTimeInterval = newdf.SmallestTimeInterval.astype(int)
print("Pre-Process Finished!")
print(time.time()-start_time)

# One Hot Encoding
print("Starting Encoding")
from sklearn import preprocessing

## Label Encoder
categoricalNames = {}
objectList = list(newdf.loc[:, newdf.dtypes == object])
print(objectList)
objectList.extend(['Result'])
print(objectList)

for feature in objectList:
    le = preprocessing.LabelEncoder()
    le.fit(newdf.loc[:, feature])
    newdf.loc[:,feature] = le.transform(newdf.loc[:,feature])
    categoricalNames[feature] = le.classes_
	
print(newdf[objectList].head())
print(newdf[objectList].info())
print("Label Encoder Finish")
print(time.time()-start_time)


ohe = preprocessing.OneHotEncoder()
ohe.fit_transform(newdf[objectList])
print('One Hot Encoding Finished!')
print(time.time()-start_time)


# Defining X & y
X = newdf.drop('Result',1)
y = newdf[['Result']]

print("X columns:")
print(list(X))

#mutual_info_classif
print("This is mutual_info_classif")
print("Selected Feature:")
mic = mutual_info_classif(X,y)
print(mic)
print("mic done!")
print(time.time()-start_time)
'''
[ 0.1107  0.1376  0.0389  0.1355  0.0053  0.0034  0.0017  0.0057  0.0106
  0.0127  0.1193  0.0597  0.1655  0.2409]

'''

#chi2
chi= chi2(X,y)
print("This is chi2")
print(chi[0])
print("p value:")
print(chi[1])
print("chi2 done!")
print(time.time()-start_time)


# SelectKBest
skb = SelectKBest()
skb.fit(X,y)
skb_feature = skb.transform(X)
print("This is SelectKBest")
print("Feaure score:")
print(skb.scores_)
print("Selected Feature:")
print(skb_feature)
print("skb done!")
print(time.time()-start_time)


# PCA
pca = PCA() 
pca.fit(X)
print("This is PCA")
print("PCA Explained Variance Ratio: ", pca.explained_variance_ratio_)
print("pca done!")
print(time.time()-start_time)


# Writing File
np.savetxt('FS_mic_1.txt', mic)
np.savetxt('FS_skb_scores_1.txt', skb.scores_)
np.savetxt('FS_skb_feature_1.txt', skb_feature)
np.savetxt("FS_PCA_explained_variance_ratio_1.txt", \
pca.explained_variance_ratio_)
np.savetxt('FS_chi2_1.txt', chi)

print("All Done!")
print(time.time()-start_time)
