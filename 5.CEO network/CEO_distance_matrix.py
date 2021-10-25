# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 14:42:47 2021

This code is to calculate the distance among different CEOs addresses

@author: haoch
"""
import pandas as pd
import requests
import geopandas as gpd
import numpy as np
import json
from shapely.geometry import Point
import multiprocessing as mp
from itertools import repeat
pd.set_option('display.max_columns',None)
#%%
# drop duplicates for home address
# Home address is got from lexis parsing
Home_add=demo.sort_values(by=['Street Address','GPS coordinates'])\
    .drop_duplicates(subset=['LexID','Street Address'])
    
#delete address before 2000
Home_add['End year']=Home_add['End year'].apply(lambda x:int(x) if str(x).isdigit() else np.nan)
demo=Home_add[Home_add['End year']>2000]
#%%
# google geocode to get coordinates
def getcoor(address):
    key=' '
    address=address.replace('#','NO.')
    url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)

    req = requests.get(url)
    res = req.json()
    if res['status']=='OK':
        result = res['results'][0]
        coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
        new_add=result['formatted_address']
        return coor, new_add
    else:
        print(url,address)
        
#%%
#get coordinates for all address
def get_all_coor(demo):
    for ind, row in demo.iterrows():
        coor=demo.loc[ind,'GPS coordinates']
        add=demo.loc[ind,'Street Address']
        if pd.isna(coor):
            coor2,new_add=getcoor(add)
            demo.loc[ind,'GPS coordinates']=coor2 
            demo.loc[ind,'Google address']=new_add
        else:
            continue
    pass
#%%
# comparing original address zip code with google returned address
# flag the wrong coordinates returned by google
#mis_coor= demo[~demo['Google address'].isnull()]
#mis_coor['zip flag']=mis_coor.apply(lambda x: 1 \
#            if str(x['Google address']).split(',')[-2].split()[-1]\
#                in x['Street Address'].split()[-1].split('-')[0] else 0,\
#            axis=1)
#%%
# build geo-dataframe
def build_df (test,name):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    geometry=test[name].apply(lambda x: Point(json.loads(x)[::-1]))
    test = gpd.GeoDataFrame(test, crs="EPSG:4326", geometry=geometry)
    return test
#%%
# calculate distance among CEOs house address
# output distance smaller than 1km
# write distance smaller than 200 km into csv
def get_dis_matrix(LexID,demo):
    path='D:/RA Python/CEO/Instrument/Distance/data/log.txt'
    open(path, "a").write(LexID+'\n')
    result_list=[]
    test=demo[demo['LexID']==LexID].to_crs(3310)
    target=demo[demo['LexID']!=LexID].to_crs(3310)
    for ind, row in test.iterrows():
        p=test.loc[ind,'geometry']
        coor=test.loc[ind,'GPS coordinates']
        LexID=test.loc[ind,'LexID']
        add=test.loc[ind,'Street Address']
        target['Distance']=target['geometry'].apply(lambda x: p.distance(x))/1000
        result=target[['LexID','Street Address','Distance','GPS coordinates']]
        result['LexID A']=LexID
        result['Street Address A']=add
        result['coordinate A']=coor
        result_list.append(result)
    final=pd.concat(result_list).reset_index(drop=True)
    final=final.rename(columns=\
        {"LexID": "LexID B", "Street Address": "Street Address B",'GPS coordinates':'coordinate B'})
    cut=final[final['Distance']<=1]
    final = final.drop(['LexID A','coordinate A','coordinate B'], 1)
    final=final[final['Distance']<=200]
    path='Distance/Distance result/'+LexID+'.csv'
    final.to_csv(path,sep='|')
    return cut
#%%
# multiprocess function
def multi_matrix():
    Home=pd.read_csv('D:/RA Python/CEO/Instrument/Distance/result/Home.csv',sep='|',dtype = str)
    Home=build_df(Home,'GPS coordinates')
    ID_list=Home['LexID'].to_list()
    ID_list=list(set(ID_list))
    pool=mp.Pool(processes=5)
    mult_res = pool.starmap(get_dis_matrix, zip(ID_list, repeat(Home)))
    pool.close()
    pool.join()
    return mult_res
#%%
if __name__ == '__main__':
    res_list=multi_matrix()
