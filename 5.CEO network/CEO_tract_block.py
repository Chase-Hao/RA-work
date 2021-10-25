# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 09:41:36 2021

Get tractID and blockID for all addresses

@author: haoch
"""
import pandas as pd
import geopandas as gpd
import numpy as np
import json
from shapely.geometry import Point
import glob
pd.set_option('display.max_columns',None)
#%%
# load all tract files
# extract first two digit of GEOID, transfer to state name
def get_tract_file():
    state_map=json.load(open('D:/RA Python/CEO/Instrument/code/geoid.json'))
    res_list=[]
    shp_list=glob.glob('D:/RA Python/CEO/Instrument/Distance/tract data/*/*.shp') 
    for files in shp_list:
        res_list.append(gpd.read_file(files,\
                ignore_fields=['NAMELSAD','NAME','ALAND','AWATER','INTPTLAT','INTPTLON']))
        open('D:/RA Python/CEO/Instrument/Distance/data/log.txt', "a").write(files+'\n')
    tract=pd.concat(res_list)
    tract['state_id']=tract['STATEFP'].apply(lambda x: state_map.get(x))
    tract = tract[['state_id','GEOID','geometry']]
    return tract

# for each address, get tract GEOID
# search tract based on state first
def get_tract(test,tract):
    group=tract.groupby('STATEFP')
    for ind,row in test.iterrows():
        state=test.loc[ind,'Address State']
        p=test.loc[ind,'geometry']
        if 'TractID' in test.columns:
            TractID=test.loc[ind,'TractID']
        else:
            TractID=np.nan
        if pd.isna(TractID):
            try:
                data=group.get_group(state)
                BlockID=data[data['geometry'].contains(p)]['GEOID'].reset_index(drop=True)[0]
                test.loc[ind,'TractID']=BlockID 
            except Exception as e:
                print(ind,e)
    return test

# Build geo-dataframe
def build_df (test,name):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    geometry=test[name].apply(lambda x: Point(json.loads(x)[::-1]))
    test = gpd.GeoDataFrame(test, crs="EPSG:4326", geometry=geometry)
    return test

# After block GEOID for address
# search for tractID first
def get_block(test,data):
    data['tract_id']=data['GEOID'].apply(lambda x: x[0:11]) 
    group=data.groupby('tract_id')
    for ind,row in test.iterrows():
        #state=test.loc[ind,'Address State']
        p=test.loc[ind,'geometry']
        tract=test.loc[ind,'TractID']
        if pd.isna(tract):
            continue
        else:
            try:
                data=group.get_group(tract)
                BlockID=data[data['geometry'].contains(p)]['GEOID'].reset_index(drop=True)[0]
                test.loc[ind,'BlockID']=BlockID
            except Exception as e:
                print(ind,e)
    return test

#%%
# get zip code from tract geoID
def match_zip(Home):
    tract2zip=pd.read_excel('Distance/ZIP_TRACT_062021.xlsx', usecols=[0,1], dtype = str)
    tract2zip=tract2zip.rename(columns={"zip":"Tract_zip","tract":"TractID"})
    tract2zip=tract2zip.drop_duplicates(subset=['TractID'])
    if 'Tract_zip' in Home.columns:
        Home.drop('Tract_zip', axis=1, inplace=True)
    Home=Home.merge(tract2zip,how='left')
    Home['zip flag']=Home.apply(lambda x: 1 \
                    if str(x['Tract_zip'])[0:4] in x['Street Address'].split()[-1].split('-')[0] else 0,\
                        axis=1)
    return Home
#%%
# transform all number into string
def to_string(Tract):
    Tract['GVKEY']=Tract['GVKEY'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
    Tract['year_beg_CEO']=Tract['year_beg_CEO'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
    Tract['year_left_ceo']=Tract['year_left_ceo'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
    Tract['Commuter']=Tract['Commuter'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x))else x)
    pass
    
