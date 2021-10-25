# -*- coding: utf-8 -*-
"""
Created on Sun May 30 18:28:28 2021

This code is to get HQ address/ mail address in header 

@author: haoch
"""
#%%
import re
import unicodedata
import time
import pandas as pd
import datetime
#%%
# the most direct way to get business address
def find_bus(Tt1):
#T1['mail_add'] = T1['mail_add'].astype('object')
    for ind, row in Tt1.iterrows():
        header=Tt1.loc[ind,'header']
    #mail=re.findall("MAIL ADDRESS:\s*((?:.|\n)*?)$",header)
        bus=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)\n\n", header)
        if len(bus)==0:
            bus=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)$",header)
        Tt1.loc[ind,'flag_bus']=len(bus)
        try:
            # get all sperated information, like street, city and etc
            for detail in bus:
                colnm=detail.replace("\t","").split(':')[0]
                cont=detail.replace("\t","").split(':')[1]
                Tt1.loc[ind,colnm]=cont
        except:
            continue        

    pass

#%%
# get mail address(imporved method)
def get_mailadd(demo1):
    for ind, row in demo1.iterrows():
        mail=re.findall("MAIL ADDRESS:\s*((?:.|\n)*?)\n\n", demo1.loc[ind,'header'])
        if len(mail)==0:
            mail=re.findall("MAIL ADDRESS:\s*((?:.|\n)*?)\n\n\n\n", demo1.loc[ind,'header'])
        if len(mail)==0:
            mail=re.findall("MAIL ADDRESS:\s*((?:.|\n)*?)$", demo1.loc[ind,'header'])
        try: 
            add_d=mail[0].split('\n')
            if len(add_d)==0:
                add_d=mail[0].split('\n\n')
        except:
            continue   
        if len(add_d)>=2:
            try:
                for detail in add_d:
                    colnm=detail.replace("\t","").split(':')[0]
                    cont=detail.replace("\t","").split(':')[1]
                    if len(colnm)>0:
                        demo1.loc[ind,colnm]=cont
            except:
                continue
    pass

#%%
# get business address (improved method)
def getadd2(demo1):
    for ind, row in demo1.iterrows():
        bus_add=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)\n\n", demo1.loc[ind,'header'])
        if len(bus_add)==0:
            bus_add=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)\n\n\n\n", demo1.loc[ind,'header'])
        if len(bus_add)==0:
            bus_add=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)$", demo1.loc[ind,'header'])
        try: 
            add_d=bus_add[0].split('\n')
            if len(add_d)<2:
                add_d=bus_add[0].split('\n\n')
        except:
            continue   
        if len(add_d)>=2:
                for detail in add_d:
                    try:
                        colnm=detail.replace("\t","").split(':')[0]
                        cont=detail.replace("\t","").split(':')[1]
                    except:
                        continue
                    if len(colnm)>0:
                        demo1.loc[ind,colnm]=cont
    pass
#na_demo=Final[demo1['STATE'].isnull()]
getadd2(Final_na)

#%% 
#remove tags or other special characters in parsed address
def rm_tag(test):
    p = re.compile(r'<\s*((?:.|\n)*?)>')
    for ind, row in test.iterrows():
        p1=p.sub('', str(test.loc[ind,'leads']))
        p2=p1.replace('&nbsp;','').replace('&nbsp','')
        test.loc[ind,'leads']=p2
    pass

#%%
# if there are multiple companies in header
def get_add3(test):
    for ind, row in test.iterrows():
        com_list=test.loc[ind,'header'].split('FILER:')
        if '\n\n\n\n' in test.loc[ind,'header']:
            com_list=test.loc[ind,'header'].replace('\n\n','\n').split('FILER:')
        name=test.loc[ind,'Company']
        for com in com_list:
            if name.lower() in com.lower():
                header=com
        bus_add=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)\n\n", header)
        if len(bus_add)==0:
                bus_add=re.findall("BUSINESS ADDRESS:\s*((?:.|\n)*?)$", header)
        try: 
            add_d=bus_add[0].split('\n')
        except:
            continue
        for detail in add_d:
            if ":" in detail:
                colnm=detail.replace("\t","").split(':')[0]
                cont=detail.replace("\t","").split(':')[1]
                test.loc[ind,colnm]=cont
    pass
#%%
# get report time
def get_rep_time(demo_na):
    demo_na['header'] = demo_na['header'].apply( lambda x: x.replace('\\n','\n').replace('\\t','\t'))
    demo_na['report_time'] = demo_na['header'].apply( lambda x: re.findall("CONFORMED PERIOD OF REPORT:\s*((?:.)*?)\\n", x))
    demo_na['report_time'] = demo_na['report_time'].apply( lambda x: 0 if len(x) == 0 else x[0])
    demo_na['report_time'] = demo_na['report_time'].apply( lambda x: datetime.datetime.strptime(str(x) , '%Y%m%d').date())
    return demo_na
#%%
def get_flag_HQ_relocate(demo):
    demo=demo.sort_values(by=['CIK','report_time']).reset_index(drop=True)
    grouped=demo.groupby('CIK')
    for group, df in grouped:
        ind0=df.index[0]
        add0=df.loc[ind0,'STREET 1']
        for ind, row in df.iterrows():
            add=df.loc[ind,'STREET 1']
            if add0!=add:
                demo.loc[ind,'flag']=1
            else:
                demo.loc[ind,'flag']=0
            add0=add
    return demo
#%%
demo.to_csv('D:/RA Python/CES data/data/addressfull.csv')
#%%
#%%
# create full address variable
for ind, row in demo.iterrows():
    old_add=str(demo.loc[ind,'STREET 1'])+','+\
            str(demo.loc[ind,'STREET 2'])+','+\
            str(demo.loc[ind,'CITY'])+','+\
            str(demo.loc[ind,'STATE'])+','+\
            str(demo.loc[ind,'ZIP'])
    old_add=old_add.replace('nan','')
    old_add=re.sub(',+', ',', old_add)
    demo.loc[ind,'header_address']=old_add
#%%
test=demo[demo['report_yr']>1999]
#%%
test.to_csv('D:\\RA Python\\CEO\\HQ.csv',sep='|')