# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 00:59:14 2021

This code is to download all 2020 cencus data in tract level

@author: haoch
"""
import multiprocessing as mp
import requests
from bs4 import BeautifulSoup
#%%
#get all links of files
def get_download_lnk():
    link_list = []
    url = 'https://www2.census.gov/geo/tiger/TIGER2020/TRACT/'
    r = requests.get(url)
    data = r.text
    soup = BeautifulSoup(data, features="html.parser").find_all('a')
    for link in soup:
        res = link.get('href')
        if type(res) == str:
            if '.zip' in res:
                res = url+res
                link_list.append(res)
            else:
                continue
        else:
            continue
    return link_list
#%%
# download all links
def download_file(url):
    path = 'D:/RA Python/CEO/Instrument/Distance/tract data/'+url.split('/')[-1]
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        open(path, 'wb').write(r.content)
    pass
#%%
# multiprocessing to expedite
def multicore():
    file_list=get_download_lnk()
    pool=mp.Pool(processes=4)
    pool.map(download_file,file_list)
    pool.close()
    pool.join()
    pass
#%%
if __name__ == '__main__':
    multicore()
