# -*- coding: utf-8 -*-
"""
Created on Sun Jun 9 18:28:28 2021
# This file is get url of all companies' 10k

@author: haoch
"""

# %%
import requests
import pandas as pd
import urllib
from bs4 import BeautifulSoup
import time

# %%
def get_all_url():
    year_list=[]
    for year in range(2000,2021):
        com_list=[]
        search_form_type = r"https://www.sec.gov/cgi-bin/srch-edgar"
        #here
        search_num_par = {'text': 'form-type=(10-K)',
                      'first': year,
                      'last': year}
        res_num = requests.get(url=search_form_type, params=search_num_par)
        num_soup=BeautifulSoup(res_num.content, 'lxml')
        total_num=int(num_soup.find('b').get_text())
        start=1
        print(year)
        while len(com_list)<total_num:
            soup,leng=get_soup(start,year)
            if leng==0:
                print('none result in',year,start)
                break
            else:
                com_list=com_list+get_url(soup)
                start=start+100
        year_list.append(com_list)
    return year_list
# make the request
# %%
def get_url(soup):
    companies = []
    for link in soup.find_all('entry'):
        out = link.find('title').get_text().split(' - ')
        url = link.find('link')['href']
        cik = url.split('/')[4]
        date = link.find('updated').get_text()
        com = {'CIK': cik, 'Company': out[1],
               'type': out[0], 'date': date, 'url': base_url+url}
        companies.append(com)
    return companies
#%%
def get_soup(start,year):
    search_form_type = r"https://www.sec.gov/cgi-bin/srch-edgar"
    search_form_params = {'text': 'form-type=(10-K)',
                      'output': 'atom',
                      'count': 99,
                      'start':start,
                      'first': year,
                      'last': year}
    response = requests.get(url=search_form_type, params=search_form_params)
    soup = BeautifulSoup(response.content, 'lxml')
    leng=len(soup.find_all('entry'))
    return soup, leng
#%%
