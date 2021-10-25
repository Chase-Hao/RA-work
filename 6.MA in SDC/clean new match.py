# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 21:10:13 2021

This file is to clean result, find unmatched, combine matches with CEO info

@author: haoch
"""
#company suffix: inc, co, corp, llc, group, Incorporated
import pandas as pd
from fuzzywuzzy import fuzz
import re
import numpy as np
from cleanco import prepare_terms, basename
#%%
# load SDC data (new match SDC.py)
SDC=get_SDC()
#%%
#Centrue Financial Corp,Ottawa, Illinois
err['com2']=err['Company'].apply(lambda x: basename(x, terms, prefix=False, middle=False, suffix=True))
#%%
#set flag for other RA work if it appears in new match
def get_flag_list(result):
    last_work=pd.read_excel('D:/RA Python/CEO/M&A filter/email file/Deal Master Dan Edit.xlsx',sheet_name='Completed Deals')
    err_list=[]
    last=last_work.dropna(subset=['Commuter CEO Name'])
    res_list=[]
    for ind, row in last.iterrows():
        com_nm=last.loc[ind,'Target Firm Name']
        CEO_nm=last.loc[ind,'Commuter CEO Name']
        res=fuzzy_search(com_nm,result,'CEO Name')
        if len(res)==0:
            err_list.append(ind)
            continue
        res=fuzzy_search(CEO_nm,res,'Target Full Name match')
        if len(res)==0:
            err_list.append(ind)
            continue
        res_list.append(res)
    flag_res=pd.concat(res_list)
    err_data=last_work.iloc[err_list]
    err_data=err_data.replace({r'\t': ''}, regex=True)
    err_data['Commuter CEO Name']=err_data['Commuter CEO Name'].apply(lambda x: x.strip())
    return flag_res,err_data
#%%
#new fuzzy research
def fuzzy_search(com_nm,sample,col):
    name=re.sub('[^a-zA-Z0-9\s]', '', com_nm.strip())
    terms = prepare_terms()
    name=basename(name, terms, prefix=False, middle=False, suffix=True)
    sample[('similarity'+col)]=sample[col].apply(lambda x:\
            fuzz.ratio(re.sub('[^a-zA-Z0-9\s]', '',\
            basename(str(x), terms, prefix=False, middle=False, suffix=True)).lower(),name.lower()))
    res=sample[sample[('similarity'+col)]>85]
    #res['first name flag']=res[col].apply(lambda x: x.split(' ')[0]==com_nm.split(' ')[0])
    colnm=col+' match'
    res[colnm]=com_nm
    return res
#%%
#get SDC No for other RAs' work
def get_SDC_No(SDC,last):
    err_list=[]
    res_list=[]
    #last=pd.read_excel('D:/RA Python/CEO/M&A filter/email file/Deal Master Dan Edit.xlsx')
    for ind, row in last.iterrows():
        target=last.loc[ind,'Target Firm Name']
        acquire=last.loc[ind,'Acquiror Firm Name']
        year=last.loc[ind,'Year Transaction Closed']
        
        res=fuzzy_search(target,SDC,'Target Full Name')
        if len(res)==0:
            err_list.append(ind)
            continue
        res['Acquiror']=acquire
        res['year']=year
        res_list.append(res)
    res_list=pd.concat(res_list)
    err_data=last.iloc[err_list]
    return res_list,err_data
#%%
# get records unfound in SDC from other RAs' work
def get_SDC_missing(SDC,last):
    err_list=[]
    res_list=[]
    #last=pd.read_excel('D:/RA Python/CEO/M&A filter/email file/Deal Master Dan Edit.xlsx')
    for ind, row in last.iterrows():
        target=last.loc[ind,'Target Firm Name']
        acquire=last.loc[ind,'Acquiror Firm Name']
        year=last.loc[ind,'Year Transaction Closed']
        
        res=fuzzy_search(acquire,SDC,'Acquiror Full Name')
        if len(res)==0:
            err_list.append(ind)
            continue
        res['Target Firm Name match']=target
        res['year']=year
        res_list.append(res)
    res_list=pd.concat(res_list)
    err_data=last.iloc[err_list]
    return res_list,err_data
#%%
# manual work here
#%%
#load munual check error
err=pd.read_excel('D:/RA Python/CEO/M&A filter/result/new SDC/err v1.xlsx')
err_match2=get_new_match(err,SDC,'Target Firm Name')
#%%
# set flag if name are same, and effictive is matched, and effective year are closed
# between new matches and other RAs' work
res_list['first name flag']=res_list.apply(\
        lambda x: x['Target Full Name'].split(' ')[0].lower()==x['Target Firm Name match'].split(' ')[0].lower(),axis=1)
res_list['year flag']=res_list.apply(\
        lambda x: x['Date Effective'].year==x['year'] if type(x['year'])==int else np.nan,axis=1)
res_list['year flag']=res_list.apply(\
        lambda x: x['Date Effective'].year in range(int(x['year'])-2, int(x['year'])+2) \
            if type(x['year'])==float and not pd.isna(x['year']) else np.nan,axis=1)
#%%
#This is munual check part
# Search in acquiror
d=SDC[SDC['Acquiror Full Name'].apply(lambda x: 'SERVICEWARE TECHNOLOGIES'.lower() in str(x).lower())].reset_index(drop=True)
# Search in target 
d=SDC[SDC['Target Full Name'].apply(lambda x: 'iMedia'.lower() in str(x).lower())].reset_index(drop=True)
# search in Synopsis
d=SDC[SDC['Deal Synopsis'].apply(lambda x: 'iMedia Brands'.lower() in str(x).lower())].reset_index(drop=True)
# search target/acquiror first, then search synopsis
d1=d[d['Deal Synopsis'].apply(lambda x: 'Invicta'.lower() in str(x).lower())].reset_index(drop=True)
#%%
# get CEO name and company name map from commuter file
# to match company name for matches in SDC
def get_name_dict(last_work):
    nm_dict = dict()
    for ind, row in last_work.iterrows():
        target=last_work.loc[ind,'Target Firm Name']
        name=re.sub('[^a-zA-Z0-9\s]', '', target.strip())
        terms = prepare_terms()
        name=basename(name, terms, prefix=False, middle=False, suffix=True)
        nm_dict.update({name:target})
    return nm_dict

res_list['Target name matched']=res_list['Target Full Name match'].apply(lambda x: nm_dict.get(x))
