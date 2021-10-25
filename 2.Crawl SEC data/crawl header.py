# -*- coding: utf-8 -*-
"""
Created on Tue May 25 22:48:09 2021

1. crawl companies' header info from SEC
2. generate indicator for records which contains keywords such as 'relocate headquarter'

@author: haoch
"""
# %%
import re
import requests
import unicodedata
from bs4 import BeautifulSoup
import time
import pandas as pd
# %%
# get contant between header tag
def get_cont(url):
    response = requests.get(url)
    soup = str(response.content)
    header = re.findall("<SEC-HEADER>\s*((?:.|\n)*?)</SEC-HEADER>", soup)
    return header, soup

# %%
# iterate url file from url.py, to get all header content
def spider(test):
    for ind, row in test.iterrows():
        try:
            url = row['url']
            url = url.replace('-index.htm', '.txt')
            b = 0
            header, soup = get_cont(url)
            if len(header) == 0:
                while len(header) == 0:
                    b = b+1
                    header, soup = get_cont(url)
                    time.sleep(0.1)
                    if len(header) > 0:
                        test.loc[ind, 'header'] = header[0]
                        #test.loc[ind, 'leads'] = get_para(soup)
                        break
                    if b > 4:
                        err_list.append({'index': ind, 'url': url})
                        time.sleep(0.1)
                        break
            if len(header) > 0:
                test.loc[ind, 'header'] = header[0]
                # time limit for requesting SEC
                #test.loc[ind, 'leads'] = get_para(soup)
                time.sleep(0.1)
        except:
            err_list.append({'index': ind, 'url': url})
            continue
    pass

# %%
# search content that two keywords within certain range
def find_relate(key1, key2, text):
    i = text.find(key1)
    while i != -1:
        key=''
        for k in key2:
            if i-600 > 0:
                j = text.find(k, i-300, i+300)
            else:
                j = text.find(k, 0, i+600)
            if j != -1:
                key=key+k+","
        i = text.find(key1, i+1)
    return -1
# %%
# get paragraph which contains keywords indicating relocation of HQ
def get_para(content):
    x = find_relate(
        'relocat', ['headquart', 'office', 'corporate'], content.lower())
    if x == -1:
        return 0
    else:
        paragraph = content[x-300:x+300]
        return paragraph
# %%
spider(test)
