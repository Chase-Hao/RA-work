# -*- coding: utf-8 -*-
"""
Created on Thu May  6 16:33:09 2021

@author: chao27
"""
import time
import requests
import pandas as pd
import geopandas as gpd
import numpy as np
import json
from shapely.geometry import Polygon,Point,LineString, MultiPolygon
from geopandas import GeoDataFrame
pd.set_option('display.max_columns',None)
#%%
HQ_add=pd.read_excel('D:\\RA Python\\CES data\\data\\10K v3.xlsx')
#%%
test=demo[demo['last_this_diff_add_flag(ratio<0.8)']>0]
#%%
def change_na(add):
    add=str(add)
    if len(add)<=3:
        add=''
    return add
#%%
for group, df in grouped:
    ind0=df.index[0]
    add0=df.loc[ind0,'10k_address']
    add0=change_na(add0)
    for ind, row in df.iterrows():
        add=df.loc[ind,'10k_address']
        add=change_na(add)
        ratio=similar(add0.lower(),add.lower())
        demo.loc[ind,'last_year_this_year_similarity']=ratio
        if ratio<0.8:
            demo.loc[ind,'last_this_diff_add_flag(ratio<0.8)']=1
            demo.loc[ind0,'last_this_diff_add_flag(ratio<0.8)']=2
        else:
            demo.loc[ind,'last_this_diff_add_flag(ratio<0.8)']=0
        ind0=ind
        add0=add
test=demo[demo['last_this_diff_add_flag(ratio<0.8)']>0]
print('get coor')
getcoor(test)
#%%
def getcoor(data):
    key=''
    for i, row in data.iterrows() :
        address=data.loc[i,'10k_address']
        address=change_na(address)
        if len(address)>100 or len(address)<20:
            address=data.loc[i,'old_address']
        address=address.replace('#','NO.')
        url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)
        req = requests.get(url)
        res = req.json()
        if res['status']=='OK':
            result = res['results'][0]
            coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
            data.loc[i,'cor']=coor
            data.loc[i,'google address']=result['formatted_address']
        else:
            print(i,address)
    pass

#%%
def build_df(test,name):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    geometry=test[name].apply(lambda x: Point(json.loads(x)[::-1]))

    test = GeoDataFrame(test, crs="EPSG:4326", geometry=geometry).to_crs(3310)
    return test

#%%
def cal_dis(test,target):
    dis=test.distance(target)/1609.34
    return dis
#%%
#Filter coordinates
last2=test[~test['cor'].isnull()]
last1=last2[~last2['10k_address'].isnull()]
last=last1[last1['10k_address']!='no address']
#%%
last=build_df(last,'cor')
last=last.sort_values(by=['CIK','report_time'])
grouped=last.groupby('CIK')

#%%
for group, df in grouped:
    ind0=df.index[0]
    last_cor=df.loc[ind0,'geometry']
    for ind, row in df.iterrows():
        this=df.loc[ind,'geometry']
        dis=cal_dis(last_cor,this)
        last.loc[ind,'change_distance']=dis
        ind0=ind
        last_cor=this
#%%
def combine(demo,last1,last2,test,last):
    list1=demo[demo['last_this_diff_add_flag(ratio<0.8)']==0]
    list2=last1[last1['10k_address']=='no address']
    list3=last2[last2['10k_address'].isnull()]
    list4=test[test['cor'].isnull()]
    final=pd.concat([last,list1,list2,list3,list4])
    return final
#%%
last=last.drop(['geometry'],axis=1)
#%%
final=combine(demo,last1,last2,test,last)
final=final.fillna('-')
final = final.replace('\n','', regex=True)
final.to_csv('D:/RA Python/CES data/data/10-K Address_US_distance.csv',sep='|')

#%%
last = last.replace('\n','', regex=True)
last.to_csv('D:/RA Python/CES data/data/10-K Address_smaller.csv',sep='|')
