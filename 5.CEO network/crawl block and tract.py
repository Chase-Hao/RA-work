# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 01:02:49 2021

This code is to crawl cencus tract and block information based on coordinates
From https://geocoding.geo.census.gov/geocoder/geographies/coordinates

@author: haoch
"""

import requests
import json
#%%
# request geo census website 
def get_geoids(lng, lat, benchmark = 4, vintage = 4):
    params = "?x={}&y={}&benchmark={}&vintage={}&format=json".format(lng, lat, benchmark, vintage)
    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    r = requests.get(url + params)
    if r.status_code!= 200:
        return {}
    else:
        return json.loads(r.text)

# get GEOID by geometry level which can be found in cencus web
# e.g. 'Census Tracts','2020 Census Blocks'
def pull_geoids(lng, lat, geometry_levels):
    geoid_dict=get_geoids(lng, lat, benchmark = 4, vintage = 4)
    ids = {}
    geoids = geoid_dict['result']['geographies']
    for level in geometry_levels:
        res = dict(filter(lambda item: level in item[0], geoids.items()))
        key = list(res.keys())[0]
        ids[level] = res[key][0]['GEOID']
    return ids
