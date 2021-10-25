# -*- coding: utf-8 -*-
"""
Created on Sat Jun 19 09:38:34 2021

@author: haoch
"""
#%%
import os
import re
from dateutil import parser
import pandas as pd
import requests
import numpy as np
pd.set_option('display.max_columns',None)
#%%
#deal with vote address
# dataf is all parsed Lexis data
vote=dataf[dataf["Address"]=="Voter"]
mask = (vote["End date"] ==0) | (vote["End date"] > 200000)
vote=vote.loc[mask]
#%%
def get_date_num(month,year):
    try:
        if not str(year).isdigit() :
            return 0
        if month=='':
           month=0
        month='{:02}'.format(int(month))
        return int(str(int(year))+month)
    except Exception as e:
        print(str(e))
        print(month,'|',year)
#%%
def get_vote_start_date(vote):
    for ind, row in vote.iterrows():
        start_mon=vote.loc[ind,'Start month']
        start_year=vote.loc[ind,'Start year']
        vote.loc[ind,'Start date']=get_date_num(start_mon,start_year)
    pass
def get_vote_end_date(vote):
    for ind, row in vote.iterrows():
        start_mon=vote.loc[ind,'End month']
        start_year=vote.loc[ind,'End year']
        vote.loc[ind,'End date']=get_date_num(start_mon,start_year)
    pass

#%%
def unique_vote_add(vote):
    grouped=vote.groupby(['LexID','Street Address'])
    newlist=[]
    for ind, g in grouped:
        max_start_ind=g['Start date'].idxmax()
        start_mon=g.loc[max_start_ind,'Start month']
        start_year=vote.loc[max_start_ind,'Start year']
        max_end_ind=g['End date'].idxmax()
        row=g.loc[max_end_ind].to_dict()
        row['Start month']=start_mon
        row['Start year']=start_year
        newlist.append(row)
    new_vote=pd.DataFrame.from_dict(newlist)
    return new_vote
#%%
get_vote_start_date(vote)
get_vote_end_date(vote)
new_vote=unique_vote_add(vote)
#%%
data2=pd.concat([new_vote,dataf[dataf["Address"]!="Voter"]])
data=data2[data2["Street Address"]!='']
#%%
def getcoor(address):
    key=''
    address=address.replace('#','NO.')
    url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)
    req = requests.get(url)
    res = req.json()
    if res['status']=='OK':
        result = res['results'][0]
        coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
        return coor
    else:
        print(url,address)

#%%
def cor(data):
    for ind, row in data.iterrows():
        address=data.loc[ind,'Street Address']
        coor=getcoor(address)
        data.loc[ind,'GPS coordinates']=coor
    pass
#%%
cor(dataf)
#%%
def after2000(data):
    data1=data[~data['End year'].isna()]
    data2=data1[data1['End year']!='']
    data2['End year'] = pd.to_numeric(data2['End year'])
    data2=data2[data2['End year']>=2000]
    data2=pd.concat([data2,data1[data1['End year']=='']])
    data_new=pd.concat([data2,data[data['End year'].isna()]])
    return data_new
#%%
voter=after2000(voter)
dataf=pd.concat([dataf,CEO_add[0:0]])
#%%
CEO_add=pd.concat([dataf,CEO_add])
#%%
data_new=data_new.reset_index(drop=True)
data=data_new[data_new['GPS coordinates'].isna()]
data0=pd.concat([data,data_new[~data_new['GPS coordinates'].isna()]])
data=data0[data0['GPS coordinates'].isna()]
demo=pd.concat([data,data0[~data0['GPS coordinates'].isna()]])
data1=demo[demo['GPS coordinates'].isna()]

#%%
data=pd.concat([data1,demo[~demo['GPS coordinates'].isna()]])
#%%
t=data[data['GPS coordinates'].isna()]
#%%
data.to_csv('D:\\RA Python\\CEO\\Lexis\\add_coor.csv',sep='|')
#%%
#%%
demo=pd.read_excel('D:\\RA Python\\CEO\\HQ_add.xlsx')
#%%
