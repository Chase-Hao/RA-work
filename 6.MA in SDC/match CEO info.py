# -*- coding: utf-8 -*-
"""
Created on Sat Oct  9 20:22:10 2021

This file is to get CEOs information for other RAs work
using fuzzy match

@author: haoch
"""
import pandas as pd
from fuzzywuzzy import fuzz
import re
pd.set_option('display.max_columns',None)
#%%
#from match for SDC.py
commuter_master = get_commuter_master()
last_work=pd.read_excel('D:/RA Python/CEO/M&A filter/Deal Master Dan Edit.xlsx')
#%%
# get CEOs info from commuter file
def get_CEO_map(commuter_master=get_commuter_master()):
    CEO_map=commuter_master[['FirstName','MiddleName','LastName','LexID','Company','Starting time of tenure','Ending time of tenure']]\
        .drop_duplicates()
    CEO_map['LexID']=CEO_map['LexID'].apply(lambda x: str(x))
    CEO_map['CEO Name']=CEO_map.apply(lambda x: x['FirstName'].strip()+' '+x['LastName'].strip(),axis=1)
    return CEO_map
#dup=CEO_map[CEO_map.duplicated(subset=['LexID','Company'],keep=False)]
#%%
#match CEOs info with other RAs' work
def match_CEO(CEO_map=et_CEO_map(),target):
    res_list=[]
    for ind, row in CEO_map.iterrows():
        start=CEO_map.loc[ind,'Starting time of tenure']
        end=CEO_map.loc[ind,'Ending time of tenure']
        com_nm=CEO_map.loc[ind,'Company']
        
        FirstName=CEO_map.loc[ind,'FirstName']
        MiddleName=CEO_map.loc[ind,'MiddleName']
        LastName=CEO_map.loc[ind,'LastName']
        LexID=CEO_map.loc[ind,'LexID']
        CEOName=FirstName+' '+LastName
        
        res=target[target['commuter company']==com_nm]
        if len(res)==0:
            continue
        res['FirstName']=FirstName
        res['MiddleName']=MiddleName
        res['LastName']=LastName
        res['LexID']=LexID
        res['CEO Name']=CEOName
        
        res=res[(res['DateAnnounced']>=start) & (res['DateAnnounced']<=end)]
        res_list.append(res)
    result=pd.concat(res_list)
    result=result.drop_duplicates(subset=target.columns.difference(['MiddleName']))
    return result
result=pd.concat([match_CEO(CEO_map,target[target['CEO Name'].isna()]),\
                  target[~target['CEO Name'].isna()]])
#d=result[result.index.duplicated(keep=False)]
#%%
# fuzzy search
def fuzzy_search(com_nm,sample,col):
    name=re.sub('[^a-zA-Z0-9\s]', '', com_nm.strip())
    sample[('similarity'+col)]=sample[col].apply(lambda x: fuzz.ratio(re.sub('[^a-zA-Z0-9\s]', '', str(x)).lower(),name.lower()))
    res=sample[sample[('similarity'+col)]>=85]
    colnm=col+' match'
    res[colnm]=name
    return res

#%%
# find corresponding CEOs records in new matching result from other RAs work
def get_flag_list(result,last_work):
    err_list=[]
    last=last_work.dropna(subset=['Commuter CEO Name'])
    res_list=[]
    for ind, row in last.iterrows():
        com_nm=last.loc[ind,'Target Firm Name']
        CEO_nm=last.loc[ind,'Commuter CEO Name']
        res=fuzzy_search(com_nm,result,'commuter company')
        if len(res)==0:
            err_list.append(ind)
            continue
        res=fuzzy_search(CEO_nm,res,'CEO Name')
        if len(res)==0:
            #err_list.append(ind)
    err_data=last_work.iloc[err_l
            print(ind,CEO_nm)
            continue
        res_list.append(res)
    flag_res=pd.concat(res_list)ist]
    return flag_res,err_data
#%%
#flag other RA work
flag_res,err_data=get_flag_list(result,last_work)
flag_list=flag_res.index.tolist()
result.loc[flag_list,'last work flag']=1
#%%
def get_err_list(err_list):
    CEO_map=get_CEO_map(commuter_master)
    err=err_list.merge(CEO_map,how='left',left_on='Commuter CEO Name',right_on='CEO Name')
    err_list['Commuter CEO Name']=err_list['Commuter CEO Name'].apply(lambda x: x.strip())
    err['similarity']=err.apply(lambda x:\
            fuzz.ratio(re.sub('[^a-zA-Z0-9\s]', '', str(x['Target Firm Name'])).lower(),\
                       re.sub('[^a-zA-Z0-9\s]', '', str(x['Company'])).lower()),axis=1)
    return err

#%%
result.to_csv('D:/RA Python/CEO/M&A filter/result/v1_CEO_info.csv',sep='|')
#%%
#remove duplicates in new match data, remove similarity
result=result.drop_duplicates()
new_match2=pd.concat([result,result_000]).drop_duplicates(subset=result.columns.difference(['similarity']),keep=False)
#%%
last_map=last_work[['Commuter CEO Name','Target Firm Name']].drop_duplicates()
#%%
pd.concat([err_match,result]).to_csv('D:/RA Python/CEO/M&A filter/result/new SDC/first_match.csv',sep='|')



