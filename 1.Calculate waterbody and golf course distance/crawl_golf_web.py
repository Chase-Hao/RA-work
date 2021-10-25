# -*- coding: utf-8 -*-
"""
Created on Wed May  5 10:56:16 2021
Crawling golf course information from pga website. PGA has updated its own website now. So this code is expired.
@author: chao27
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import lxml
#%%
# the website structure is https://www.pga.com/state/city/golf_course
# I get the state first and then city and golf course
def get_state(url):
    states=[]
    r = requests.get(url)
    r.encoding = 'utf-8'
    html = r.text
    soup = BeautifulSoup(html, "lxml")
    
    city_s=soup.find('ul',{'data-cy':'states'}).find_all('li')
    for s in city_s: 
        states.append(s.find('a').get('href'))
    return states
#%%
states=get_state('https://www.pga.com')
#%%
def get_cities(url):
    l1=[]
    r = requests.get(url)
    r.encoding = 'utf-8'
    html = r.text
    soup = BeautifulSoup(html, "lxml")
    city_s=soup.find('ul',{'data-cy':'city-list'}).find_all('li')
    for c in city_s: 
        con=c.find('a').contents
        href=c.find('a').get('href')
        l1.append({'city':con[0],'state':url[25:len(url)],'num':int(con[4]),"href":href})   
    return l1

#%%
def look_add(url,addr):
    r = requests.get(url)
    r.encoding = 'utf-8'
    html = r.text
    soup = BeautifulSoup(html, "lxml")
    search_add = soup.find(class_='MuiGrid-root MuiGrid-container MuiGrid-spacing-xs-2')
    for child in search_add.children:
        name=child.find("h5").string
 #childrens.append(child.contents[0])
        x=0
        for j in child.contents[0].descendants:
            x+=1
            if x == 11:
                first=''.join(map(str, j.contents))
                #.replace("  <br/>", ", ").replace("   ", ", ")
                continue
        addr.append({"name":name,"addr":first,"state":url.split('/')[-2],"city":url.split('/')[-1]})
    return addr
#%%
def get_all_url():
    url1='https://www.pga.com'
    states=get_state(url1) 
    addr=[]
    err=[]
    for s in states:
        url=url1+s
        city=get_cities(url)
        for i in city:
            url2=url1+i.get('href')
            try:
                addr.extend(look_add(url2,[]))
            except Exception:
                print(url2, Exception)
                err.append(url2)
                continue
    return addr,err
#%%
addr,err = get_all_url()
addr_Full=pd.DataFrame(addr)

#%%
#cleaning crawled data
addr_Full['address']=addr_Full['addr'].apply(lambda x: x.replace("  <br/>", ", ").replace("   ", ", "))
err_add=addr_Full[addr_Full['address'].str.contains("<br/>")]
citynum['address']=citynum['href'].apply(lambda x: x.split('/')[-1])
city=addr_Full['city'].to_list()
errcity = citynum[-citynum.address.isin(city)]
#%%
correct2=[]
url='https://www.pga.com'
errcity
for i in errcity['href'].to_list():
    url3=url+i
    correct2.extend(look_add(url3,[]))
#%%
Final=pd.read_csv(filepath_or_buffer="addr_Full2.csv")
Final.state.value_counts()