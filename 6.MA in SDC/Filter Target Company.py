# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 17:56:08 2021

# This file is to search M&A transaction in 'target company' of SDC data

@author: haoch
"""
import os
import pandas as pd
import numpy as np
from dateutil import parser
from fuzzywuzzy import fuzz
from pandas.tseries.offsets import MonthEnd
import multiprocessing as mp
import re
#%%
# Load and clean commuter file
def get_commuter_master():
    commuter_master=pd.read_excel('D:/RA Python/CEO/Commuting CEO Database Sept 15.xlsx')
    commuter_master['Ending year of tenure']=commuter_master['Ending year of tenure'].replace({'Present': 2021}, regex=True)
    commuter_master['Ending month of tenure']=commuter_master['Ending month of tenure'].replace({'Present': 12}, regex=True)
    commuter_master['Commuting ending month']=commuter_master['Commuting ending month'].replace({'Present': 2021}, regex=True)
    commuter_master['Commuting ending year']=commuter_master['Commuting ending year'].replace({'Present': 12}, regex=True)
    commuter_master['Starting month of tenure']=commuter_master['Starting month of tenure'].apply(lambda x: 1 if pd.isna(x) else int(x))
    commuter_master['Ending month of tenure']=commuter_master['Ending month of tenure'].apply(lambda x: 1 if pd.isna(x) else int(x))
    
    commuter_master['Starting time of tenure']=pd.to_datetime(commuter_master['Starting year of tenure'].astype(str)  + commuter_master['Starting month of tenure'].astype(str), format='%Y%m')
    commuter_master['Ending time of tenure']=pd.to_datetime(commuter_master['Ending year of tenure'].astype(str)  + commuter_master['Ending month of tenure'].astype(str), format='%Y%m')+ MonthEnd(1)
    return commuter_master

#%%
# load and clean SDC data
# This is first version of SDC data, divided in different files
def get_all_SDC(og_path):
    data_list=[]
    for file in os.listdir(og_path):
        if file.endswith(".csv"):
            sample = pd.read_csv(og_path+'/'+file)
            sample.columns = sample.columns.str.replace('\n', '')
            sample.columns=sample.columns.str.strip()
    
            sample['Rank Date']=sample['Rank Date'].apply(lambda x: x if pd.isna(x) else parser.parse(x))
            sample['DateAnnounced']=sample['DateAnnounced'].apply(lambda x: x if pd.isna(x) else parser.parse(x))
            sample['DateEffective']=sample['DateEffective'].apply(lambda x: x if pd.isna(x) else parser.parse(x))
            sample['DateEffective/Unconditional']=sample['DateEffective/Unconditional'].apply(lambda x: x if pd.isna(x) else parser.parse(x))
            sample['DateWithdrawn']=sample['DateWithdrawn'].apply(lambda x: x if pd.isna(x) else parser.parse(x))
            sample['TargetCompany Date  of Fin.']=sample['TargetCompany Date  of Fin.'].apply(lambda x: x if pd.isna(x) else parser.parse(x))
            sample=sample.replace({r'\n': ' '}, regex=True)
            data_list.append(sample)
    SDC=pd.concat(data_list)
    return SDC
#SDC=get_all_SDC('D:/RA Python/CEO/M&A filter/SDC data 2018')
#%%
# search based on companies' name in 'target full name' of SDC
def get_all_match(sample):
    commuter_master=get_commuter_master()
    res_list=[]
    for ind, row in commuter_master.iterrows():
        start=commuter_master.loc[ind,'Starting time of tenure']
        end=commuter_master.loc[ind,'Ending time of tenure']
        com_nm=commuter_master.loc[ind,'Company']
        
        res=fuzzy_search_company(com_nm,sample)
        res=res[(res['DateAnnounced']>=start) & (res['DateAnnounced']<=end)]
        res_list.append(res)
    result=pd.concat(res_list)
    return result

#%%     
# fuzzy search
def fuzzy_search_company(com_nm,sample):
    name=re.sub('[^a-zA-Z0-9\s]', '', com_nm.strip() )
    sample['similarity']=sample['Target Name']\
        .apply(lambda x: fuzz.ratio(re.sub('[^a-zA-Z0-9\s]', '', str(x)).lower(),name.lower()))
    res=sample[sample['similarity']>89]
    res['commuter company']=com_nm
    return res
#%%
# divide SDC into 4 tables to do multiprocessing
def multicore():
    table=get_all_SDC('D:/RA Python/CEO/M&A filter/SDC data 2018')
    p=mp.Pool(processes=4)
    split_dfs = np.array_split(table,4)    
    pool_results = p.map(get_all_match, split_dfs)
    p.close()
    p.join()
    return pool_results
#%%
if __name__ == '__main__': 
    res_list=multicore()
    result=pd.concat(res_list)
    #result.to_csv('D:/RA Python/CEO/M&A filter/result/v1.csv',sep='|')