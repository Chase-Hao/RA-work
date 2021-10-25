# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 16:31:28 2021

@author: haoch
"""
import pandas as pd
from fuzzywuzzy import fuzz
import re
from pandas.tseries.offsets import MonthEnd
from cleanco import prepare_terms, basename
import multiprocessing as mp
import numpy as np
import os
#%%
# loading and cleaning commuter data
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

# loading and cleaning SDC data
def get_SDC():
    SDC_list=[]
    for file in os.listdir("D:\\RA Python\\M&A filter\\SDC new"):
        if file.endswith(".xlsx"):
            path=os.path.join("D:\\RA Python\\M&A filter\\SDC new", file)
            unit=pd.read_excel(path, sheet_name=None)
            SDC_list.extend([v for v in unit.values() if len(v)>0])
    SDC=pd.concat(SDC_list)
    SDC.columns = SDC.columns.str.replace('\n', '')
    SDC.columns=SDC.columns.str.strip()
    SDC=SDC.replace({r'\n': ' '}, regex=True)
    return SDC
#%%
# fuzzy search after removing special characters
# remove companies' suffix like llc, corp
def fuzzy_search(com_nm,sample,col):
    name=re.sub('[^a-zA-Z0-9\s]', '', com_nm.strip())
    terms = prepare_terms()
    name=basename(name, terms, prefix=False, middle=False, suffix=True)
    sample[('similarity'+col)]=sample[col].apply(lambda x:\
            fuzz.ratio(re.sub('[^a-zA-Z0-9\s]', '',\
            basename(str(x), terms, prefix=False, middle=False, suffix=True)).lower(),name.lower()))
    res=sample[sample[('similarity'+col)]>85]
    colnm=col+' match'
    res[colnm]=name
    return res
#%%
# this match is based on companies' name
def get_new_match(sample):
    targetnm='Company'
    commuter_master=get_commuter_master()
    res_list=[]
    for ind, row in commuter_master.iterrows():
        #get CEO info from master
        start=commuter_master.loc[ind,'Starting time of tenure']
        end=commuter_master.loc[ind,'Ending time of tenure']
        com_nm=commuter_master.loc[ind,targetnm]
        FirstName=commuter_master.loc[ind,'FirstName']
        MiddleName=commuter_master.loc[ind,'MiddleName']
        LastName=commuter_master.loc[ind,'LastName']
        LexID=commuter_master.loc[ind,'LexID']
        CEOName=FirstName+' '+LastName
        
        res=fuzzy_search(com_nm,sample,'Target Full Name')
        #log unmatched company name
        if len(res)==0:
            path='D:/RA Python/CEO/M&A filter/result/new SDC/unmatched company.txt'
            open(path, "a").write(str(ind)+','+str(com_nm)+'\n')
            continue
        #res=res[(res['Date Announced']>=start) & (res['Date Announced']<=end)]
        #add CEO info into result
        res['FirstName']=FirstName
        res['MiddleName']=MiddleName
        res['LastName']=LastName
        res['LexID']=LexID
        res['CEO Name']=CEOName
        res['Starting time of tenure']=start
        res['Ending time of tenure']=end
        
        #res['Starting time of tenure']=start
        #res['Ending time of tenure']=end       
        
        res_list.append(res)
    result=pd.concat(res_list)
    result=result.drop_duplicates()
    return result

#%%
# drop all records which appear in first munual match file (Deal master) from commuter file
# to avoid duplicating search
# use fuzzy match
def remove_last_work():
    result=get_commuter_master()
    result['CEO Name']=result.apply(lambda x: x['FirstName']+' '+x['LastName'],axis=1)
    result['ind']=result.index
    
    deal_master=pd.read_excel('D:/RA Python/CEO/M&A filter/email file/Deal Master Dan Edit.xlsx')
    deal_master=deal_master.replace({r'\t': ' '}, regex=True)
    last=deal_master.dropna(subset=['Commuter CEO Name'])

    err_list=[]
    res_list=[]
    for ind, row in last.iterrows():
        target=last.loc[ind,'Target Firm Name']
        acquior=last.loc[ind,'Acquiror Firm Name']
        CEO_nm=last.loc[ind,'Commuter CEO Name']
        res=fuzzy_search(CEO_nm,result,'CEO Name')
        if len(res)==0:
            err_list.append(ind)
            continue
        res=fuzzy_search(target,res,'Company')
        if len(res)==0:
            #err_list.append(ind)
            res=fuzzy_search(acquior,res,'Company')
            if len(res)==0:
                err_list.append(ind)
                continue
        res_list.append(res)
        
    flag_res=pd.concat(res_list).drop_duplicates()
    flag_list=flag_res.index.to_list()
    remain=result[~result['ind'].isin(flag_list)].iloc[:,:-2]
    #err_data=last.iloc[err_list]
    remain.to_csv('D:/RA Python/M&A filter/email file/remaining commuter.csv', sep='|',index=False)
    pass
#%%
# remove cusip match from remaining cusip match result
def get_remain(last_remain=pd.read_csv('D:/RA Python/M&A filter/email file/remaining commuter.csv', sep='|'),\
               cusip_match=pd.read_excel('D:/RA Python/M&A filter/result/munual check/cusip match increment.xlsx')):
    target_list=[]
    for ind, row in cusip_match.iterrows():
        CEO_Name=cusip_match.loc[ind,'CEO Name']
        Company=cusip_match.loc[ind,'company match']
        unit=last_remain[(last_remain['CEO Name']==CEO_Name)&(last_remain['Company']==Company)]
        target_list.extend(unit.index.to_list())
    remain=last_remain[~last_remain.index.isin(target_list)]
    return remain
#%%
# search for company based on 6-digit cusip
# cusip is always recycled, but it's unique for a certain company in certain year
def match_cusip(SDC,commuter):
    cusip_map=pd.read_csv('D:/RA Python/M&A filter/email file/cik-cusip-maps.csv')
    #commuter=pd.read_csv('D:/RA Python/CEO/M&A filter/email file/remaining commuter.csv', sep='|')
    commuter=commuter[~commuter['CIK'].isna()]
    cusip_map['cik']=cusip_map['cik'].apply(lambda x: int(x))
    cusip_map['cusip6']=cusip_map['cusip6'].apply(lambda x: str(x).lower().strip())
    SDC['Target 6-digit CUSIP']=SDC['Target 6-digit CUSIP'].apply(lambda x: str(x).lower())
    SDC['Acquiror 6-digit CUSIP']=SDC['Acquiror 6-digit CUSIP'].apply(lambda x: str(x).lower())
    res_list=[]
    for ind, row in commuter.iterrows():
        cik=int(commuter.loc[ind,'CIK'])
        cusip_list=cusip_map[cusip_map['cik']==cik]['cusip6'].to_list()
        start=commuter.loc[ind,'Starting time of tenure']
        end=commuter.loc[ind,'Ending time of tenure']
        
        com_nm=commuter.loc[ind,'Company']
        LexID=commuter.loc[ind,'LexID']
        CEOName=commuter.loc[ind,'CEO Name'] 
        
        #res=SDC[SDC['Target 6-digit CUSIP'].isin(cusip_list)]
        res=SDC[SDC['Acquiror 6-digit CUSIP'].isin(cusip_list)]
        if len(res)==0:
            continue
        res=res[(res['Date Announced']>=start) & (res['Date Announced']<=end)]
        res['LexID']=LexID
        res['CEO Name']=CEOName
        res['Starting time of tenure']=start
        res['Ending time of tenure']=end
        res['company match']=com_nm
            
        res_list.append(res)
    result=pd.concat(res_list).drop_duplicates()
    # match target
    #result['Similarity']=result.apply(lambda x:fuzz.ratio(x['Target Full Name'].lower(),x['company match'].lower()),axis=1)
    #match acquiror
    result['Similarity']=result.apply(lambda x:fuzz.ratio(x['Acquiror Full Name'].lower(),x['company match'].lower()),axis=1)
        
    return result
#%%
def multicore():
    table=get_SDC()
    p=mp.Pool(processes=5)
    split_dfs = np.array_split(table,5)    
    pool_results = p.map(get_new_match, split_dfs)
    p.close()
    p.join()
    return pool_results
#%%
if __name__ == '__main__': 
    res_list=multicore()
    raw_result=pd.concat(res_list)
