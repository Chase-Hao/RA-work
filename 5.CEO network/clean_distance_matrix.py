# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 22:24:32 2021

This file is to clean result got from CEO_distance_matrix
-merge matching result with CEOs' info
-filter out the records that CEOs living periods are overlapped 

@author: haoch
"""
import pandas as pd
import numpy as np
#%%
# res_list is the result got from distance_matrix calculation 
Distance_res=pd.concat(res_list).reset_index(drop=True)
df1=pd.DataFrame(np.sort(Distance_res[['Street Address A','Street Address B']], axis=1))
Distance = Distance_res[~df1.duplicated()]
Distance_res=Distance
#%%
#get CEO lexID, company, tenure period map
#From commuter master file and non-commuter
def get_map(CEO_master,commuter,Home):
    commuter['Full Name']=commuter.apply(lambda x: x['FirstName']+' '+x['MiddleName']+' '+x['LastName']\
                                         if not pd.isna(x['MiddleName']) else\
                                         x['FirstName']+' '+x['LastName'],axis=1)
    map1=CEO_master[['LN_ID','CONAME','GVKEY','year_beg_CEO','year_left_ceo']]
    map2=commuter[['LexID','Company','Gvkey','Starting year of tenure','Ending year of tenure']]

    map1.rename(columns={'LN_ID':'LexID','CONAME':'Company'}, inplace=True)
    map2.rename(columns={'Starting year of tenure':'year_beg_CEO',\
                     'Ending year of tenure':'year_left_ceo','Gvkey':'GVKEY'}\
            ,inplace=True)

    map1['Commuter']=0
    map2['Commuter']=1
    map2['LexID']=map2['LexID'].apply(lambda x : str(int(x)))
    map1['LexID']=map1['LexID'].apply(lambda x : str(x).split(',')[0])
    map1['LexID']=map1['LexID'].apply(lambda x : str(int(x)))

    CEO_map=pd.concat([map2,map1],ignore_index=True)
    CEO_map['LexID']=CEO_map['LexID'].apply(lambda x : int(x) if str(x).isdigit() else x)
    CEO_map=CEO_map.drop_duplicates(subset=['LexID','GVKEY'],keep='first')
    CEO_map['LexID']=CEO_map['LexID'].apply(lambda x : str(x))
    return CEO_map
#%%
# merge CEO name into CEO_map
def merge_CEOmap(CEO_master,commuter,Home,Distance_res):
    CEO_map=get_map(CEO_master,commuter,Home)
    mapA = CEO_map.add_suffix(' A')
    mapB = CEO_map.add_suffix(' B')
    Distance_res['LexID B']=Distance_res['LexID B'].apply(lambda x: str(int(x)))
    Distance_res['LexID A']=Distance_res['LexID A'].apply(lambda x: str(int(x)))
    Distance_res=Distance_res.merge(mapA,how='left')
    Distance_res=Distance_res.merge(mapB,how='left')
    return Distance_res
#%%
# merge CEOs name with distance matrix 
# Home is the CEOs Lexis records dataframe
def get_name(Home,Distance_res):
    Home['LexID']=Home['LexID'].apply(lambda x: str(int(x)))
    name_map=Home[['Full Name','LexID']].drop_duplicates(subset=['LexID'])
    name_mapA = name_map.add_suffix(' A')
    name_mapB = name_map.add_suffix(' B')
    Distance_res=Distance_res.merge(name_mapA,how='left')
    Distance_res=Distance_res.merge(name_mapB,how='left')
    return Distance_res
#%%
Distance_res=get_name(Home,Distance_res)
#%%
# only keep useful columns
cols=['Full Name A','LexID A','Street Address A','Company A','GVKEY A','year_beg_CEO A','year_left_ceo A','Commuter A',\
'Full Name B', 'LexID B','Street Address B', 'Company B', 'GVKEY B','year_beg_CEO B','year_left_ceo B', 'Commuter B',\
'Distance']
Distance=Distance_res[cols]
#%%
data=get_tract(test,group)
group=data.groupby('state_id')
Home.drop('TractID', axis=1, inplace=True)
Home=get_tract(Home,group)

#%%
data = gpd.read_file("D:/RA Python/CEO/Instrument/Distance/US_block.gdb",\
                     driver='FileGDB',\
                         ignore_fields=['SUFFIX','NAME','ALAND','AWATER','INTPTLAT','INTPTLON'])
#%%
devision=Home[['Full Name','LexID','Street Address','Address State','TractID','BlockID']]
#%%
def get_block(devision,BlockID):
    block_map=devision.groupby(by=BlockID,as_index=False)['LexID']\
        .agg({'LexID': pd.Series.nunique})
    block_map=block_map[block_map['LexID']>1][BlockID].to_list()
    Block=devision[devision[BlockID].isin(block_map)]
    Block=Block.sort_values(by=BlockID)
    return Block
#%%
#transform all numeric into consistent string format 
Distance['GVKEY A']=Distance['GVKEY A'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
Distance['GVKEY B']=Distance['GVKEY B'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)

Distance['year_beg_CEO A']=Distance['year_beg_CEO A'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
Distance['year_left_ceo A']=Distance['year_left_ceo A'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
Distance['Commuter A']=Distance['Commuter A'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x))else x)

Distance['year_beg_CEO B']=Distance['year_beg_CEO B'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
Distance['year_left_ceo B']=Distance['year_left_ceo B'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x)) else x)
Distance['Commuter B']=Distance['Commuter B'].apply(lambda x: str(int(x)) if (type(x)!=str and not pd.isna(x))else x)

#%%
# get records that CEOs have overlapping living period
def get_year_map(add_time_map,Distance100):
    add_time_map=add_time_map.drop_duplicates(subset=['LexID','Street Address'])
    add_time_map['Start year']=add_time_map['Start year']\
        .apply(lambda x:int(x) if str(x).isdigit() else np.nan)
    mapA = add_time_map.add_suffix(' A')
    mapB = add_time_map.add_suffix(' B')
    Distance=Distance100.merge(mapA,how='left')
    Distance=Distance.merge(mapB,how='left')
    Distance['flag']=Distance.apply(lambda x: 0 if (x['End year A']<x['Start year B'] or\
                x['End year B']<x['Start year A']) else 1 , axis=1)
    Distance=Distance[Distance['flag']==1]
    return Distance

