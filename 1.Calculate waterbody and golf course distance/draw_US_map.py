# -*- coding: utf-8 -*-
"""
Created on Thu Sep 23 16:04:58 2021
get US map data from open street map 
rel["ISO3166-2"~"^US"]
   [admin_level=4]
   [type=boundary]
   [boundary=administrative];
out geom;

@author: haoch
"""
import geopandas as gpd
import pandas as pd
import json
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt
import requests
pd.set_option('display.max_columns',None)
#%%
US_land=gpd.read_file('D:/RA Python/Distance Test1/data/land-polygons/land_polygons.shp')
states=gpd.read_file('D:/RA Python/Distance Test1/data/US state.geojson')
water=gpd.read_file('D:/RA Python/Distance Test1/data/water/water.shp')
#%%
commuter=pd.read_excel('D:/RA Python/CEO/Commuting CEO Database Sept 15.xlsx')
#%%
commuter=build_df (commuter[~commuter['HM coordinates'].isna()],'HM coordinates')
#%%
# cleaning state border and water data
water['area']=water['geometry'].area
demo=water[water['area']>0.005]
states=states[~states['ISO3166-2'].isna()]
states=states[['name','geometry']]  
state_map=pd.DataFrame(json.load(open('D:/RA Python/Distance Test1/states_t.json')))
states=states.merge(state_map,how='left')
#%%
box=(-171,-65,18,72)
box_main=( -127.24501986859698,-66.82022008880983,24.592042056035858,49.57150902558099)
box_AK=(-168.8994735772468,-140.42291339011805,56.096842410860646,71.4273423975611)
box_HI=(-160.5817726160622, -154.67112856487577,18.774812693579573,22.36384807798491)
#%%
def map_plot(states,US_land,water,commuter,cor,box):
    xmin, xmax, ymin,ymax = box
    fig = plt.figure(figsize=(200,200))
    fig.set_facecolor(cor)
    ax = fig.add_subplot()
    ax.set_facecolor(cor)
    #plot US boundary
    US_land.plot(ax=ax,facecolor='white',edgecolor=cor)
    #plot water boundary
    water.plot(ax=ax, color=cor,edgecolor=cor)
    #plot states
    states.plot(ax=ax,facecolor='none',edgecolor='grey',linewidth=18,alpha=0.5)
    #plot commuter
    commuter.plot(ax=ax,facecolor='#1E90FF',alpha=0.5,markersize=1000)
    for ind, row in states.iterrows():
        s=get_s(row['abbreviation'])
        h,v=get_h_v(row['abbreviation'])
        ax.annotate(text=row['abbreviation'], xy=row['geometry'].centroid.coords[0],ha=h,va=v,size=s)
    ax.set_xlim(xmin - .1, xmax + .1) 
    ax.set_ylim(ymin - .1, ymax + .1)
    plt.savefig("world.jpg")

#%%
def HI_plot(states,commuter,cor,box):
    xmin, xmax, ymin,ymax = box
    fig = plt.figure(figsize=(70,70))
    fig.set_facecolor(cor)
    ax = fig.add_subplot()
    ax.set_facecolor(cor)
    HI=states[states['abbreviation']=='HI']
    d=commuter[commuter['Home residence state']=='HI']
    HI.plot(ax=ax,facecolor='white',edgecolor='grey')
    HI.plot(ax=ax,facecolor='none',edgecolor='grey',linewidth=40,alpha=0.5)
    d.plot(ax=ax,facecolor='#1E90FF',alpha=0.5,markersize=20000)
    ax.annotate(text='HI', xy=(-158.42845247845528,20.224630399050355),ha='left',va='bottom',size=500)
    ax.set_xlim(xmin - .1, xmax + .1) 
    ax.set_ylim(ymin - .1, ymax + .1)
    plt.savefig("HI.jpg")
#%%
#Adjust annodate size
def get_s(name):
    if name in ['DC','DE','RI']:
        return 75
    elif name =='AK':
        return 400
    elif name =='HI':
        return 150
    else: return 100

def get_h_v(name):
    if name =='DC':
            h='left'
            v='bottom'
    elif name =='MD':
            h='center'
            v='bottom'
    else:
            h='center'
            v='center'
    return h,v
#%%
def build_df (test,name):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    geometry=test[name].apply(lambda x: Point(json.loads(x)[::-1]))
    test = gpd.GeoDataFrame(test, crs="EPSG:4326", geometry=geometry)
    return test

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
def get_all_coor(demo):
    for ind, row in demo.iterrows():
        add=demo.loc[ind,'Home address']
        if not pd.isna(add):
            coor2,new_add=getcoor(add)
            demo.loc[ind,'HM coordinates']=coor2 
            demo.loc[ind,'Google address']=new_add
        else:
            continue
    pass

#%%
map_plot(states,US_land,water,commuter,"#c6c6c6",box)
#%%
HI_plot(states,commuter,"#c6c6c6",box_HI)
