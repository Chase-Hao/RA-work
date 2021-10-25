# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 00:41:37 2021

Get voter history based on registrantID generated from voteID_match

@author: haoch
"""
import pandas as pd
import glob
import multiprocessing as mp
pd.set_option('display.max_columns',None)

#%%
def get_state_file(F_match,state):
    state_g=F_match.groupby('State')
    file=state_g.get_group(state)
    return file
#%%
# read all files and process them to get result
# It doesn't work in most cases because different states have different variable names
# def read_files(file):
#     file_list=glob.glob(file+"/*.txt")
#     df_list=[]
#     for f in file_list:
#         FL=pd.read_table(f,sep=',',error_bad_lines=False,dtype = str)
#         print(f)
#         df_list.append(get_result_wy(FL,WY_match))
#     return pd.concat(df_list)

#%%
# filter the voter history records based on master file match result
def get_result(FL,ID,F_match,state):
    file=get_state_file(F_match,state)
    ID_map=file['RegistrantID'].to_list()
    FL=FL.applymap(str)
    FL[ID]=FL[ID].apply(lambda x: float(x) if is_float(x) else x)
    result=FL[FL[ID].isin(ID_map)]
    return result
#%%
def process_df(FL):
    ID='VUID'
    state='TX'
    F_match=pd.read_excel('match.xlsx', dtype = str)
    F_match['RegistrantID']=F_match['RegistrantID'].apply(lambda x: float(x) if is_float(x) else x)
    result = get_result(FL,ID,F_match,state)
    return result
#%%
# def get_result_wy(FL,WY_match):
#     ID_map=WY_match['VUIDNO'].to_list()
#     result=FL[FL['RegistrantID'].isin(ID_map)]
#     return result
#%%
def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

#%%
# processing large file by dividing it into different chunks
def big_process():
    file_add='D:/RA Python/CEO/Voter history/voter history/New York/AllNYSVoter_20200727.txt'
    reader = pd.read_table(file_add, sep=',',chunksize=1000000,dtype = str,header=None,error_bad_lines=False)
    pool = mp.Pool(4) # use 4 processes
    funclist = []
    for df in reader:
        #open('D:/RA Python/CEO/Voter history/data/log.txt', "a").write(str(df.loc[1,1])+'\n')
        f = pool.apply_async(process_df,[df])
        funclist.append(f)
    result_list = []
    for f in funclist:
        res=f.get()
        if len(res)<1:
            continue
        else:
            result_list.append(res)
    pool.close()
    pool.join()
    return pd.concat(result_list)

#%%
if __name__ == '__main__':
    res_NY=big_process()
    

