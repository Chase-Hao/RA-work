# -*- coding: utf-8 -*-
"""
Created on Thu May  6 16:33:09 2021

@author: chao27
"""
import time
import os
import requests
import pandas as pd
pd.set_option('display.max_columns',None)
#%%
Golf=pd.read_csv(filepath_or_buffer="U:/1RA Ran Duchin/GOLF/addr_Full2.csv")
#%%
# google geocode to get coordinates
def getcoor(data):
    key=''
    for i in range(len(data)) :
        address=data.loc[i,'address']
        add=address.split(",")
        if add[0][0:2]=='PO':
            address=data.loc[i,'name']+','+add[-3]+','+add[-2]
        url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)
        req = requests.get(url)
        res = req.json()
        if res['status']=='OK':
            result = res['results'][0]
            coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
            data.loc[i,'cor']=coor
            data.loc[i,'new']=result['formatted_address']
        else:
            print(i,address)
    return data
#%%
Test.drop('cor', axis=1, inplace=True)
#%%
Golf['new']=''
Golf['cor']=''
getcoor(Golf)
Golf.to_csv(path_or_buf="U:/1RA Ran Duchin/GOLF/golf_cor1.csv")
#%%
Golf['new'] = Golf['new'].fillna(0)
#%%
# get the coordinates from crawled golf address data
def simplecor(data):
    key=''
    for i in range(len(data)):
        if type(Golf.loc[i,'cor'])!=str:
            address=data.loc[i,'address']
            add=address.split(",")
            address=data.loc[i,'name']+','+add[-2]
            url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)
            req = requests.get(url)
            res = req.json()
            if res['status']=='OK':
                result = res['results'][0]
                coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
                data.loc[i,'cor']=coor
                data.loc[i,'new']=result['formatted_address']
            else:
                print(i,address)
#%%
simplecor(Golf)