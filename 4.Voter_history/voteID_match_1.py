# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 17:14:11 2021

This code is to match voter master files which don't contain date information
To identify CEOs' registrant ID

@author: haoch
"""
#%%
import numpy as np
import pandas as pd
import glob
import datetime
from dateutil import parser
import json
pd.set_option('display.max_columns',None)

#%%
FL=pd.read_csv('Voter history/Combined File/District of Columbia.csv',low_memory=False)
CEO_file=pd.read_csv('Voter history\data\CEO_file.csv',sep='|')
#%%
#Cleaning target 
CEO_file['Spouse last name']=CEO_file['Spouse full name'].apply(lambda x:x.split(',')[0] if len(str(x))>3 else x)
CEO_file['CEO first name']=CEO_file['Full Name'].apply(lambda x:find_first_name(x) if len(str(x))>3 else x)
CEO_file['Spouse first name']=CEO_file['Spouse full name'].apply(lambda x:find_first_name(x) if len(str(x))>3 else x)
CEO_file['Spouse DOY']=CEO_file['Spouse DOB'].apply(lambda x: x.year if type(x)==datetime.date else '')
CEO_file['Spouse DOM']=CEO_file['Spouse DOB'].apply(lambda x: x.month if type(x)==datetime.date else '')
CEO_file['DOY']=CEO_file['DOB'].apply(lambda x: x.year if type(x)==datetime.date else '')
CEO_file['DOM']=CEO_file['DOB'].apply(lambda x: x.month if type(x)==datetime.date else '')
CEO_file=CEO_file.sort_values(by=['Spouse SSN','DOB'],ascending=False).reset_index(drop=True)
CEO_file=CEO_file.drop_duplicates(subset = ["LexID"],keep='first')
#%%
#list of state for each CEO and spouse
#list of last name 
#%%
# get year of birth and month of birth from CEO file
def get_DOY(date):
    try:
        date=parser.parse(date)
        return date.year
    except:
        return ''
def get_DOM(date):
    try:
        date=parser.parse(date)
        return date.month
    except:
        return ''  

#%%
# transform a column of variable into string sperated by ';'
def get_col_list(df,colnm):
    df=df[~df[colnm].isnull()]
    col_list=df[colnm].tolist()
    col_list=list(set(col_list))
    col_list=';'.join(col_list)
    return col_list

# For addresses file 'Home', I divide this file into different group based on CEO
# for each group, extract address state column into a string separeted by ';'
def get_CEO_hm_state(Home,CEO_file):
    group_h=Home.groupby('LexID')
    for ind,row in CEO_file.iterrows():
        ID=CEO_file.loc[ind,'LexID']
        try:
            df=group_h.get_group(ID)
        except:
            continue
        state_list=get_col_list(df,'Street Address state')
        zip_list=get_col_list(df,'Zip')
        CEO_file.loc[ind,'hm state list']=state_list
        CEO_file.loc[ind,'zip list']=zip_list
    return CEO_file
# extract address city list
def get_CEO_hm_city(Home_city,CEO_file):
    group_h=Home_city.groupby('LexID')
    for ind,row in CEO_file.iterrows():
        ID=CEO_file.loc[ind,'LexID']
        try:
            df=group_h.get_group(ID)
        except:
            continue
        city_list=get_col_list(df,'city')
        CEO_file.loc[ind,'hm city list']=city_list
    return CEO_file

#%%
# match CEO based on name, zip code, city and state
def get_match(FL,demo):
    state_map=json.load(open('states_hash.json',))
    #filter based on state
    target_state=state_map.get(FL.loc[0,'State'])
    test=demo[demo['hm state list'].str.contains(target_state)]
    FL['LastName']=FL['LastName'].apply(lambda x:x.upper() if type(x)==str else x)
    if 'DOB' in FL: 
        FL['DOY']=FL['DOB'].apply(lambda x: get_DOY(x))
        FL['DOM']=FL['DOB'].apply(lambda x: get_DOM(x))
        demo=pd.merge(test,FL,left_on=['CEO last name','DOY','DOM'],right_on=['LastName','DOY','DOM'],how='inner')
    elif 'YOB' in FL:
        FL['YOB']=FL['YOB'].apply(lambda x: str(x))
        demo=pd.merge(test,FL,left_on=['CEO last name','DOY'],right_on=['LastName','YOB'],how='inner')
    else:
        print('no DOB')
        demo=pd.merge(test,FL,left_on=['CEO last name'],right_on=['LastName'],how='inner')
    if len(demo)==0:
        return pd.DataFrame()
    if 'Zip' in FL:
        demo['f']=demo.apply(lambda x: 1 if str(x['Zip'])[0:5] in x['zip list'] else 0,axis=1)
    else:
        demo['f']=1
#Because zip is more specific than city, we can ignore city if we consider zip
    # if 'City' in demo:
    #     demo['f']=demo.apply(lambda x: 1 if str(x['City']).upper() in x['hm city list'] else 0,axis=1)
    # else:
    #     demo['f']=1
    demo['f1']=demo.apply(lambda x: 1 if str(x['CEO first name']).split(' ')[0] in x['FirstName'].upper() else 0,axis=1)
    result=demo[(demo['f']==1)&(demo['f1']==1)]
    #result=demo[demo['f1']==1]
    return result

#%%
#extract all voter files
def get_res_list(demo):
    files = glob.glob("D:/RA Python/CEO/Voter history/Combined File/*.csv")
    #files = glob.glob("D:/RA Python/CEO/Voter history/test/*.csv")
    df_list=[]
    for f in files:
        print(f)
        FL = pd.read_csv(f,low_memory=False)
        result=get_match(FL,demo)
        df_list.append(result)
    return df_list

#%%
# check back to see if there are multiple records for same persons
def get_dup(test1):
    dup_list=[]
    group=test1.groupby(['LexID','State'])
    for ID, df in group:
        if len(df)>1:
            dup_list.append(df)
    return pd.concat(dup_list, axis=0)

