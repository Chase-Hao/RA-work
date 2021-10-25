# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 17:39:21 2021

@author: haoch
"""
import pandas as pd
import numpy as np
import os
import re
import json
pd.set_option('display.max_columns',None)

#%%
def main(og_path):
    err_list=[] 
    tot_list=[]
    
    for file in os.listdir(og_path):
        if file.endswith(".txt"):
            data_file=os.path.join(og_path, file)
            f = open(data_file, encoding="utf-8").read()
            try:
                LexID=find_nextn(f,f.find('LexID'),1)
                full_name=find_nextn(f,f.find('Full Name'),1)
                boat_list=get_boats_list(f)
                tot_list.append({'Full Name':full_name,"LexID":LexID,"boat_list":boat_list})
            except Exception as e:
                print(str(e))
                print(data_file)
                err_list.append(data_file)
    return tot_list,err_list

#%%

def get_df(f):
    df1=[]
    state_map=json.load(open('D:/RA Python/CEO/Distance Test1/states_hash.json',))
    for unit in f:
        LexID=unit['LexID']
        boat_list=unit['boat_list']
        for boat in boat_list:
            State=state_trans(boat["State"],state_map) 

            boat_unit={"LexID":LexID,"Address":boat['Address'],"State":State,'Use':boat["Use"],\
                       "Hull Type":boat["Hull Type"],"Vessel Year":boat["Vessel Year"],\
                           "Length(Ft-In)":boat["Length"],"Registration Issue Date":boat["Registration Issue Date"],\
                             "Registration Expiration Date":boat["Registration Expiration Date"],\
                               "Date Last Seen":boat["Date Last Seen"],\
                         }
            df1.append(boat_unit)
    return df1

def find_nextn(f,ind_tar,n):
    try:
        i=1
        b,e=next_line(f,ind_tar)
        result=f[b:e]
        while i<n:
            b,e=next_line(f,b)
            result=result+','+f[b:e]
            i=i+1
    except:
        return ''
    return result

def get_boats_list(f):
    boat_list=[]
    try:
        r=re.search("Boats -(.*?)records found",f)
        num=int(r.group(1))
        if num==0:
            return boat_list
    except Exception as e:
        print(e)
        return boat_list
    ind_start=r.end()
    ind_end=f.find('records found',ind_start)
    flist=re.split('\\n\d*?:', f[ind_start:ind_end])
    for f1 in flist:
        if len(f1)>50:
            boat_list.append(get_boat_info(f1))
    return boat_list


def re_search(pattern,f1):
    try:
        return re.search(pattern,f1).group(1)
    except:
        return ''
def state_trans(state,state_map):
    state=state.strip()
    full=state_map.get(state)
    if pd.isnull(full):
        return state
    else:
        return full

def get_boat_info(f1):
    State=re_search("State:(.*?)\\n",f1)
    Use=re_search("Use:(.*?)\\n",f1)
    Hull_Type=re_search("Hull Type:(.*?)\\n",f1)
    Vessel_Year=re_search("Vessel Year:(.*?)\\n",f1)
    Length=re_search("Length \(Ft-In\):(.*?)\\n",f1).strip()
    Registration_Issue_Date=re_search("Registration Issue Date:(.*?)\\n",f1)
    Registration_Expiration_Date=re_search("Registration Expiration Date:(.*?)\\n",f1)
    Date_Last_Seen=re_search("Date Last Seen:(.*?)\\n",f1)
    Address=re_search("Address: (.*?\\n.*?)\\n",f1)
    Address=Address.replace('\n', ',').replace('  ','')
    boat_unit={"State":State,'Use':Use,"Hull Type":Hull_Type,"Vessel Year":Vessel_Year,\
                 "Length":Length,"Registration Issue Date":Registration_Issue_Date,\
                     "Registration Expiration Date":Registration_Expiration_Date,\
                     "Date Last Seen":Date_Last_Seen,"Address":Address\
                         }
    return boat_unit

#%%
f_1,err_list=main("D:\\RA Python\\CEO\\Lexis\\Lexis Reports")
f_d,err_list=main("D:\\RA Python\\CEO\\Lexis\\Lexis Reports\\commuter\\Denis")
f_j,err_list=main("D:\\RA Python\\CEO\\Lexis\\Lexis Reports\\commuter\\Jeffery")
f_o1,err_list=main("D:\RA Python\CEO\Lexis\Lexis Reports\commuter\Olivia")
f_o2,err_list=main("D:\RA Python\CEO\Lexis\Lexis Reports\commuter\Olivia2")
f_o3,err_list=main("D:\RA Python\CEO\Lexis\Lexis Reports\commuter\Olivia2\CEO Batch 4")

Boat=pd.DataFrame(get_df(f_1+f_d+f_j+f_o1+f_o2+f_o3)).drop_duplicates()
#%%
CEO=CEO.drop_duplicates()
CEO_u=CEO.drop_duplicates(subset=['LexID'])

Boat['LexID']=Boat['LexID'].apply(lambda x : int(x))
demo=Boat.merge(CEO_u, how='outer')
#%%
demo.to_csv('D:\\RA Python\\CEO\\Instrument\\instrument_result\\boat.csv',sep='|')