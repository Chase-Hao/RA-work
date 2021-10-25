# -*- coding: utf-8 -*-
"""
Created on Wed Aug 11 16:27:00 2021

This code organizes all codes about water and gold distance caculation

@author: haoch
"""
#%%

import requests
import geopandas as gpd
import numpy as np
import pandas as pd
import json
from shapely.geometry import Polygon,Point,LineString
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt
pd.set_option('display.max_columns',None)
#%%
Com_master=pd.read_excel('Commuting CEO Database.xlsx')
Com_master['HM_coor']=np.nan
Com_master['HQ_coor']=np.nan
coast=gpd.read_file('NA_coast.shp')

#%%
#get waterbody data and caculate area of each
water=gpd.read_file('D:\\RA Python\\water\\water.shp')
water['area']=water['geometry'].to_crs({'init': 'epsg:3310'})\
               .map(lambda p: p.area / (2.58999*10**6))
#%%
#get coordiantes for HQ and Home
def getcoor(address):
    key=' '
    url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)
    req = requests.get(url)
    res = req.json()
    if res['status']=='OK':
        result = res['results'][0]
        coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
        new_add=result['formatted_address']
        print(address,'+',new_add)
        return coor
    else:
        return np.nan

#%%
#get coordinates
# it's better to use itterrows loop here
Com_master['HM_coor']=Com_master.apply(\
                        lambda x: getcoor(x['Home address'].replace('#','NO.')) \
                            if pd.isnull(x['HM_coor']) else x['HM_coor'] , axis=1)
Com_master['HQ_coor']=Com_master.apply(\
                        lambda x: getcoor(x['HQ address'].replace('#','NO.')) \
                            if pd.isnull(x['HQ_coor']) else x['HQ_coor'], axis=1)
#%%
def build_df (test,name):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    corname=name+"_coor"
    geometry=test[corname].apply(lambda x: Point(json.loads(x)[::-1]))
    test = GeoDataFrame(test, crs="EPSG:4326", geometry=geometry).to_crs(3310)
    return test

def cal_coast_dis(test,target,name):
    test=build_df (test,name)
    col_nm='ocean'+"_to_"+name+'_'+'distance'
    target=target.to_crs(3310) 
    for index, row in test.iterrows():
        p=row.loc['geometry']
        dis=target.geometry.apply(lambda x: p.distance(x)).min()/1609.34
        test.loc[index,col_nm]=dis
    return test 

def cal_golf_dis(test,target,name):
    test=build_df (test,name)
    col_nm=target.ind+"_to_"+name+'_'+'distance'
    target=target.to_crs(3310) 
    for index, row in test.iterrows():
        p=row.loc['geometry']
        dis=target.geometry.apply(lambda x: p.distance(x)).min()/1609.34
        test.loc[index,col_nm]=dis
    return test 
#%%
# build geodata and clean address
test=cal_coast_dis(Com_master,coast,'HM_coor')
test=cal_coast_dis(test,coast,'HQ_coor')
test['Home residence state']=test['Home residence state'].apply(lambda x:x.replace('?',''))
test['Headquarter state']=test['Headquarter state'].apply(lambda x:x.replace('?',''))
#%%
#build geodata for commuter file
def build_water_df (test,name,state_nm):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    corname=name+"_coor"  
    #col_nm='water'+"_to_"+name+'distance'+cut
    geometry=test[corname].apply(lambda x: Point(json.loads(x)[::-1]))
    #turn corrdinates into one measured in meter 
    test = GeoDataFrame(test, crs="EPSG:4326", geometry=geometry).to_crs(3310)
    test=test.groupby(state_nm)
    return test
#set cutoff for water area

def cal_water_dis(tes,targe,name,cut,state_nm):
    target=targe[targe['area'] >=cut].to_crs(3310)
    target=target.groupby('state') 
    test=build_water_df (tes,name,state_nm)
    tlist=[]
    col_nm='water'+"_to_"+name+'distance'+str(cut)
    for state,test1 in test: 
        try:
            target1=target.get_group(state)
        except Exception:
            tlist.append(test1)
            continue
        #col_nm=target.name1+"_to_"+name+'_'+'distance' 
        for index, row in test1.iterrows():
            p=row.loc['geometry']
            dis=target1.geometry.apply(lambda x: p.distance(x)).min()/1609.34
            test1.loc[index,col_nm]=dis
        tlist.append(test1)
    result=pd.concat(tlist, axis=0)
    return result

def cal_dis(test,target,cut,name):
    test=build_df (test,name)
    col_nm='water'+"_to_"+name+'_'+'distance'+str(cut)
    target=target[target['area'] >=cut].to_crs(3310) 
    for index, row in test.iterrows():
        p=row.loc['geometry']
        dis=target['geometry'].apply(lambda x: p.distance(x)).min()/1609.34
        test.loc[index,col_nm]=dis
    return test
#%%
test=cal_water_dis(test,water,'HM',0.1,'Home residence state')
test=cal_water_dis(test,water,'HQ',0.1,'Headquarter state')
#%%
test=cal_dis(test,water,1,'HM')
test=cal_dis(test,water,1,'HQ')
#%%
test=cal_dis(test,water,10,'HM')
test=cal_dis(test,water,10,'HQ')
#%%
Dig200=pd.read_excel('D:\\RA Python\\GOLF\\1\\data\\rank_GolfDigest.xlsx')
res200=pd.read_csv('D:\\RA Python\\GOLF\\1\\data\\residential_top200.csv')
cham1k=pd.read_csv('D:\\RA Python\\GOLF\\1\\data\\Top1000.csv')

#%%
Dig200=build_df(Dig200,'cor')
res200=build_df(res200,'cor')
cham1k=build_df(cham1k,'cor')
#%%
Dig200.ind='top200(Golf Digest)'
res200.ind='Residential200_Golf'
cham1k.ind='Golf_champion_yrd_top1000'
#%%
test=cal_golf_dis(test,Dig200,'HM')
test=cal_golf_dis(test,Dig200,'HQ')
test=cal_golf_dis(test,res200,'HM')
test=cal_golf_dis(test,res200,'HQ')
test=cal_golf_dis(test,cham1k,'HM')
test=cal_golf_dis(test,cham1k,'HQ')
test.drop('geometry', axis=1, inplace=True)
#%%
test.to_csv('D:\\RA Python\\CEO\\Instrument\\instrument_result\\new_water_golf_distance_8_13.csv',sep='|')