# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 18:21:15 2021

This code considers different situations in different states
There is a new method to match based on CEOs spouse name and DOB
If both are match, it's quite solid result
Multi-process to deal with big data
@author: haoch
"""
import multiprocessing as mp
import pandas as pd
import numpy as np
import glob
from dateutil import parser
import json
#%%
# find out states which don't have CEOs' DOB
def correct_YOB(f):
    CEO_file=pd.read_csv('Voter history\data\CEO_file.csv',sep='|')
    open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write(f+'\n')
    tt= pd.read_csv(f,nrows=10)
    if 'YOB' in tt and 'DOB' not in tt:
        FL = pd.read_csv(f,low_memory=False)
    else:
        return 'no YOB'+f
    try:
        result=get_match_city(FL,CEO_file)
    except:
        open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write('wrong'+f+'\n')
        result=f
    return result 
#%%
# processing function
def process_data(f):
    CEO_file=pd.read_csv('Voter history\data\CEO_file.csv',sep='|', dtype = str)
    open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write(f+'\n')
    FL = pd.read_csv(f,low_memory=False)
    try:
        result=get_match_spouse(FL,CEO_file)
    except:
        open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write('wrong'+f+'\n')
        result=f
    return result
#%%
# processing function based on name
def process_data_name(f):
    Name_map=pd.read_csv('D:/RA Python/CEO/Voter history/result/Name_map.csv',sep='|', dtype = str)
    Name_map=Name_map.drop(Name_map.columns[0], axis=1).drop_duplicates()
    open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write(f+'\n')
    FL = pd.read_csv(f,low_memory=False)
    try:
        result=get_match_name(FL,Name_map)
    except:
        open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write('wrong'+f+'\n')
        result=f
    return result

#%%
# get year of birth and month of birth from CEO file
def get_DOY(date):
    try:
        date=parser.parse(date)
        #return np.float64(date.year)
        return str(date.year)
    except:
        return ''
def get_DOM(date):
    try:
        date=parser.parse(date)
        #return np.float64(date.month)
        return str(date.month)
    except:
        return ''  
def get_middle_name(name):
    name=name.split(',')[1]
    try:
        name=name.split()[1]
    except:
        name=''
    return name
#%%
# match based on name, DOB
# Then match spouses' name, DOB
def get_match_spouse(FL,test):
    FL['LastName']=FL['LastName'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['FirstName']=FL['FirstName'].apply(lambda x:x.upper() if type(x)==str else x)
    if 'DOB' in FL: 
        FL['DOY']=FL['DOB'].apply(lambda x: get_DOY(x))
        FL['DOM']=FL['DOB'].apply(lambda x: get_DOM(x))
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name','DOY','DOM'],\
                      right_on=['LastName','FirstName','DOY','DOM'],how='inner')
        demo_sp=pd.merge(test.applymap(str),FL.applymap(str),left_on=['Spouse last name','Spouse first name','Spouse DOY','Spouse DOM']\
                      ,right_on=['LastName','FirstName','DOY','DOM'],how='inner')
    elif 'YOB' in FL:
        FL['YOB']=FL['YOB'].apply(lambda x: np.float64(x) if str(x).isdigit() else x)
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name','DOY']\
                      ,right_on=['LastName','FirstName','YOB'],how='inner')
        demo_sp=pd.merge(test.applymap(str),FL.applymap(str),left_on=['Spouse last name','Spouse first name','Spouse DOY']\
                      ,right_on=['LastName','FirstName','YOB'],how='inner')
    else:
        print('no DOB')
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name'],\
                      right_on=['LastName','FirstName'],how='inner')
        demo_sp=pd.merge(test.applymap(str),FL.applymap(str),left_on=['Spouse last name','Spouse first name'],\
                      right_on=['LastName','FirstName'],how='inner')
    if len(demo)==0:
        return pd.DataFrame()
    if len(demo_sp)==0:
        return pd.DataFrame()
    demo_sp=demo_sp[['LexID','street_address','Spouse first name','RegistrantID']]
    #result=demo[(demo['f']==1)&(demo['f1']==1)]
    result=demo.merge(demo_sp,on=['LexID','street_address'],how='inner')
    return result
#%%
# based on first,last, and middle name, and DOB
def get_match_name(FL,test):
    FL['LastName']=FL['LastName'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['FirstName']=FL['FirstName'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['MiddleName']=FL['MiddleName'].apply(lambda x:x.upper() if type(x)==str else x)
    test['DOY']=test['DOB'].apply(lambda x: get_DOY(x))
    test['DOM']=test['DOB'].apply(lambda x: get_DOM(x))
    test['MiddleName']=test['MiddleName'].apply(lambda x:x.upper() if type(x)==str else x)
    if 'DOB' in FL: 
        FL['DOY']=FL['DOB'].apply(lambda x: get_DOY(x))
        FL['DOM']=FL['DOB'].apply(lambda x: get_DOM(x))
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['LastName','FirstName','MiddleName','DOY','DOM'],\
                      right_on=['LastName','FirstName','MiddleName','DOY','DOM'],how='inner')
    elif 'YOB' in FL:
        FL['YOB']=FL['YOB'].apply(lambda x: np.float64(x) if str(x).isdigit() else x)
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['LastName','FirstName','MiddleName','DOY']\
                      ,right_on=['LastName','FirstName','MiddleName','YOB'],how='inner')
    else:
        print('no DOB')
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['LastName','FirstName','MiddleName'],\
                      right_on=['LastName','FirstName','MiddleName'],how='inner')

    if len(demo)==0:
        return pd.DataFrame()
    return demo
#%%
# based on first, last,DOB, zipcode, without conditioning on state
# This is the widest search
def get_match_MD(FL,test):
    FL['DOY']=FL['DOB'].apply(lambda x: get_DOY(x))
    FL['DOM']=FL['DOB'].apply(lambda x: get_DOM(x))
    FL['LastName']=FL['LastName'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['FirstName']=FL['FirstName'].apply(lambda x:x.upper() if type(x)==str else x)
    demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name','DOY','DOM'],\
                      right_on=['LastName','FirstName','DOY','DOM'],how='inner')
    if len(demo)==0:
        return pd.DataFrame()
    demo['zip_flag']=demo.apply(lambda x: 1 if str(x['Zip'])[0:4] in x['zip list'] else 0,axis=1)
    return demo

#%%
# For TX state, the variables names are different
def get_match_TX(FL,demo):   
    state_map=json.load(open('D:/RA Python/Distance Test1/states_hash.json',))
    target_state=state_map.get(FL.loc[0,'State'])
    test=demo[demo['hm state list'].str.contains(target_state)]    
    print(len(test))
    FL['lastname']=FL['lastname'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['firstname']=FL['firstname'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['DOY']=FL['birthdate'].apply(lambda x: get_DOY(x))
    FL['DOM']=FL['birthdate'].apply(lambda x: get_DOM(x))
    demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name','DOY','DOM'],\
                  right_on=['lastname','firstname','DOY','DOM'],how='inner')
    demo_sp=pd.merge(test.applymap(str),FL.applymap(str),left_on=['Spouse last name','Spouse first name','Spouse DOY','Spouse DOM']\
                     ,right_on=['lastname','firstname','DOY','DOM'],how='inner')
    print(len(demo),len(demo_sp))
    if len(demo)==0:
        return pd.DataFrame()
    sp_map=demo.merge(demo_sp[['LexID','streetnumber']],on=['LexID','streetnumber'],how='inner')
    sp_map=sp_map['SOS_VoterID'].to_list()
    demo['spouse_flag']=demo['SOS_VoterID'].apply(lambda x: 1 if x in sp_map else 0)
    demo['zip_flag']=demo.apply(lambda x: 1 if (str(x['zip'])[0:4] in x['zip list']) else 0,axis=1)
                         
    demo['city_flag']=demo.apply(lambda x: 1 if (str(x['city']).upper() in x['hm city list'].upper()) \
                                 else 0,axis=1)    

    demo['CEO middle name']=demo['Full Name'].apply(lambda x: get_middle_name(x))
    demo['middle_flag']=demo.apply(lambda x:1 if \
                    x['CEO middle name'][0:1].upper()==x['middlename'][0:1].upper()\
                        else 0, axis=1)
    # demo=demo[(demo['spouse_flag']!=0) | (demo['city_flag']!=0) \
    #                     |(demo['city_flag']!=0) | (demo['zip_flag']!=0)\
    #                     |(demo['spouse_flag']!=0)]
    return demo
#%%
# For states whose variables' names are different
# specifically for Travis count
def get_match_Diff(FL,demo,lastnm,firstnm,middlenm,registID,hm_city,hm_zip,spouse_add):   
    state_map=json.load(open('D:/RA Python/Distance Test1/states_hash.json',))
    target_state=state_map.get(FL.loc[0,'State'])
    test=demo[demo['hm state list'].str.contains(target_state)]    
    print(len(test))
    FL[lastnm]=FL[lastnm].apply(lambda x:x.upper() if type(x)==str else x)
    FL[firstnm]=FL[firstnm].apply(lambda x:x.upper() if type(x)==str else x)

    demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name'],\
                  right_on=[lastnm,firstnm],how='inner')
    demo_sp=pd.merge(test.applymap(str),FL.applymap(str),\
                     left_on=['Spouse last name','Spouse first name']\
                     ,right_on=[lastnm,firstnm],how='inner')
        
    print(len(demo),len(demo_sp))
    if len(demo)==0:
        return pd.DataFrame()
    sp_map=demo.merge(demo_sp[['LexID',spouse_add]],on=['LexID',spouse_add],how='inner')
    sp_map=sp_map[registID].to_list()
    demo['spouse_flag']=demo[registID].apply(lambda x: 1 if x in sp_map else 0)
    demo['zip_flag']=demo.apply(lambda x: 1 if (str(x[hm_zip])[0:4] in x['zip list']) else 0,axis=1)
                         
    demo['city_flag']=demo.apply(lambda x: 1 if (str(x[hm_city]).upper() in x['hm city list'].upper()) \
                                 else 0,axis=1)    

    demo['CEO middle name']=demo['Full Name'].apply(lambda x: get_middle_name(x))
    demo['middle_flag']=demo.apply(lambda x:1 if \
                    x['CEO middle name'][0:1].upper()==x[middlenm][0:1].upper()\
                        else 0, axis=1)
    # demo=demo[(demo['spouse_flag']!=0) | (demo['city_flag']!=0) \
    #                     |(demo['city_flag']!=0) | (demo['zip_flag']!=0)\
    #                     |(demo['spouse_flag']!=0)]
    return demo
#%%
# matching function when variables' names are different
def get_match(FL):
    lastnm='LAST NAME'
    firstnm='FIRST NAME'
    middlenm='MIDDLE NAME'
    registID='ASCENSION #'
    hm_city='RESIDENCE CITY'
    hm_zip='RESIDENCE ZIP'
    spouse_add='RESIDENCE ADDRESS'
    CEO_file=pd.read_csv('Voter history\data\CEO_file.csv',sep='|', dtype = str)
    result=get_match_Diff(FL,CEO_file,lastnm,firstnm,middlenm,registID,hm_city,hm_zip,spouse_add)
    return result

#%%
# return results whose all criteria are matched
def clean(demo):
    demo=demo[(demo['spouse_flag']!=0) | (demo['city_flag']!=0) \
                        |(demo['city_flag']!=0) | (demo['zip_flag']!=0)\
                        |(demo['spouse_flag']!=0)]    
    return demo
#%%
# based on state, last name, DOB, zip, city
def get_match_last_nm(FL,demo):
    state_map=json.load(open('D:/RA Python/Distance Test1/states_hash.json',))
    #filter based on state
    target_state=state_map.get(FL.loc[0,'State'])
    test=demo[demo['hm state list'].str.contains(target_state)]
    FL['LastName']=FL['LastName'].apply(lambda x:x.upper() if type(x)==str else x)
    if 'DOB' in FL: 
        FL['DOY']=FL['DOB'].apply(lambda x: get_DOY(x))
        FL['DOM']=FL['DOB'].apply(lambda x: get_DOM(x))
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','DOY','DOM'],right_on=['LastName','DOY','DOM'],how='inner')
    elif 'YOB' in FL:
        FL['YOB']=FL['YOB'].apply(lambda x: str(np.float64(x)) if str(x).isdigit() else x)
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','DOY'],right_on=['LastName','YOB'],how='inner')
    else:
        print('no DOB')
        demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name'],right_on=['LastName'],how='inner')
    if len(demo)==0:
        return pd.DataFrame()
    if 'Zip' in FL:
        demo['zip_flag']=demo.apply(lambda x: 1 if str(x['Zip'])[0:4] in x['zip list'] else 0,axis=1)
    else:
        demo['zip_flag']='no zip'
    if 'City' in demo:
        demo['city_flag']=demo.apply(lambda x: 1 if str(x['City']).upper() in x['hm city list'] else 0,axis=1)
    else:
        demo['city_flag']='no city'
    demo['f1']=demo.apply(lambda x: 1 if str(x['CEO first name']).split(' ')[0] in x['FirstName'].upper() else 0,axis=1)
    result=demo[demo['f1']==1]
    return result
#%%  
# construct multiprocessing function
def multicore():
    pool=mp.Pool(processes=4)
    file_list=glob.glob("D:/RA Python/CEO/Voter history/Combined File/*.csv")
    mult_res = pool.map(correct_YOB,file_list)
    pool.close()
    pool.join()
    return mult_res
#%%
if __name__ == '__main__':
    city2=multicore()
#%%


