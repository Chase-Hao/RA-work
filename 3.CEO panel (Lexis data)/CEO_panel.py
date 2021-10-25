# -*- coding: utf-8 -*-
"""
Created on Sat Jul 24 23:08:56 2021

This file is to generate CEOs' addresses and their companies HQ addresses
For home address, we use house with max value and with max household members
For HQ address, I use Execucomp and BoardEx data to 

@author: haoch
"""
#%%
import numpy as np
import pandas as pd
import json
pd.set_option('display.max_columns',None)
#%%
#new_h.drop(['End date','Party'], axis=1, inplace=True)
#%%
# parse home address info, get number of household member, numerize the date variable 
def parse_hm(Home_add): 
    demo=Home_add[(Home_add['Start year']!='') & (Home_add['End year']!='') ]
    demo=demo.sort_values(by=['LexID', 'Start year']).reset_index(drop=True)
    demo['Household_num']=demo['Household Members'].\
        apply(lambda x: 0 if str(x)=='nan' or str(x)=='' else len(x.split(';')))
    demo['house_value']=demo['house_value'].\
        apply(lambda x: 0 if str(x)=='nan' or str(x)=='' else int(x))
    demo['Start year']=demo['Start year'].apply(lambda x:int(x))
    demo['End year']=demo['End year'].apply(lambda x:int(x))
    demo['LexID'] = demo['LexID'].apply(lambda x: int(x) if type(x)==str else x)
    return demo
#%%
#Get unique CEOs dataset
#Each CEO only has one record
def get_CEO(demo):
    col=list(range(1,8))+[-2,-3]
    df1 = demo.iloc[:, col]
    try:
        df1.drop('commuter_file', axis=1, inplace=True)
    except:
        print('no column')
    CEO=df1.drop_duplicates()
    CEO['DOB']=CEO['DOB'].apply(lambda x:str(x))
    CEO['Spouse DOB']=CEO['Spouse DOB'].apply(lambda x:str(x))
    CEO=CEO.sort_values(by=['DOB','Spouse DOB'],ascending=False)
    CEO=CEO.reset_index(drop=True)
    #CEO.drop('commuter_file', axis=1, inplace=True)
    CEO['LexID'] = CEO['LexID'].apply(lambda x: int(x) if type(x)==str else x)
    return CEO
CEO=get_CEO(demo)
#%%
#get a list commuter lexisID regardless of commuting time
# flag those commuters
def flag_com(CEO):
    Commuter_master=pd.read_excel('D:\\RA Python\\CEO\\Instrument\\instrument_result\\Commuting CEO Database - Jeffery 0714.xlsx')
    LN_map=Commuter_master['LexID'].to_list()
    LN_map=list(set(LN_map))
    LN_map = [int(x) for x in LN_map if str(x) != 'nan']
    CEO['Commuter'] = CEO['LexID'].apply(lambda x: 1 if x in LN_map else 0)
    pass
flag_com(CEO)
#%%
# get the home address panel data for one CEO
def get_unit_panel(ID):
    grouped=demo.groupby('LexID')
    test=grouped.get_group(ID)
    panel_unit=[]
    for i in range(2000,2021):
        sub=test[(test['Start year']<=i)&(test['End year']>=i)]
        if len(sub)==0:
            panel_unit.append({"LexID":ID,"Year":i,"Max Value Address":'',"Value":"",\
                              "Household Max Address":'',"Num_member":""})
            continue    
        value_ind=sub['house_value'].idxmax()
        value=sub['house_value'].max()
        add_value_max=sub.loc[value_ind,'Street Address']
        mem_ind=sub['Household_num'].idxmax()
        mem=sub['Household_num'].max()
        add_mem_max=sub.loc[mem_ind,'Street Address']
        panel_unit.append({"LexID":ID,"Year":i,"Max Value Address":add_value_max,"Value":value,\
                           "Household Max Address":add_mem_max,"Num_member":mem})
    return panel_unit
#%%
#Get Home panel for all CEOs
def get_panel(CEO):
    panel=[]
    for ind, row in CEO.iterrows():
        ID=CEO.loc[ind,'LexID']
        unit=get_unit_panel(ID)
        panel.append(unit)
    panel_df = [item for sublist in panel for item in sublist]
    panel_df=pd.DataFrame.from_dict(panel_df).drop_duplicates()
    return panel_df
panel_df=get_panel(CEO)

#%%
# get state of every address
def find_street_state(add):
    state_map=json.load(open('states_hash.json',))
    add_list = add.split(',')
    add_list = [part.strip() for part in add_list if len(part)>1 and 'COUNTY' not in part]
    add=', '.join(add_list)
    try:
        state=add.split(',')[-1].strip().split()[0].strip()
        full=state_map.get(state)
    except Exception as e:
        print(e)
        print(add)
        
        full='wrong'
    return full
#get state of address for home panel
def parse_panel(panel_df):
    panel_df['Max Value Address state']=panel_df['Max Value Address'].apply(lambda x:find_street_state(x) if len(str(x))>3 else x)
    panel_df['Household Max Address state']=panel_df['Household Max Address'].apply(lambda x:find_street_state(x) if len(str(x))>3 else x)
    panel_df['LexID'] = panel_df['LexID'].apply(lambda x: int(x) if type(x)==str else x)
    return panel_df
panel_df=parse_panel(panel_df)

#%%
#get CEO master file and clean it
def get_master():
    Commuter_master=pd.read_excel('D:\\RA Python\\CEO\\Instrument\\instrument_result\\Commuting CEO Database - Jeffery 0714.xlsx')
    master=Commuter_master[~Commuter_master['LexID'].isnull()]
    master=master.drop_duplicates()
    master['Commuting starting year']=master['Commuting starting year'].replace('Present',2020).apply(lambda x: int(x.replace('?','')) if type(x)==str else x)
    master['Commuting ending year']=master['Commuting ending year'].replace('Present',2020).apply(lambda x: int(x.replace('?','')) if type(x)==str else x)
    master['Ending year of tenure']=master['Ending year of tenure'].replace('Present',2020).apply(lambda x: int(x.replace('?','')) if type(x)==str else x)
    return master
master=get_master()
#%%
#add commuter indicator for panel data
#If this CEO is commuting this year, the flag will be 1
#Add the companies' address in commuter_master file into this panel
# check duplicates; 
dup=pd.concat(g for _, g in master.groupby("LexID") if len(g) > 1)
#%%
def get_commuter_flag():
    m_group=master.groupby('LexID')
    panel_group=panel_df.groupby('LexID')
    for LexID,group in panel_group:
        try:
            master_unit=m_group.get_group(LexID)
        except Exception as e:
            continue
        for ind,row in master_unit.iterrows():
            #ExecID=master_unit.loc[ind,'ExecID']
            #DirectorID=master_unit.loc[ind,'DirectorID']
            CIK=master_unit.loc[ind,'CIK']
            start=master_unit.loc[ind,'Commuting starting year']       
            end=master_unit.loc[ind,'Commuting ending year']
            start1=master_unit.loc[ind,'Starting year of tenure']
            end1=master_unit.loc[ind,'Ending year of tenure']
            Home_add=master_unit.loc[ind,'Home address']    
            HQ_add=master_unit.loc[ind,'HQ address']  
            
            for i,r in group.iterrows():
                year=group.loc[i,'Year']
                if year>=start and year<=end:
                    panel_df.loc[i,'Commuter_year_flag']=1
                    #panel_df.loc[i,'ExecID']=ExecID
                    #panel_df.loc[i,'DirectorID']=DirectorID
                    panel_df.loc[i,'CIK']=CIK
                    panel_df.loc[i,'Home address']=Home_add
                    panel_df.loc[i,'HQ address_J']=HQ_add
                if year>=start1 and year<=end1:
                    panel_df.loc[i,'ceo']=1
                    #panel_df.loc[i,'ExecID']=ExecID
                    #panel_df.loc[i,'DirectorID']=DirectorID
                    panel_df.loc[i,'CIK']=CIK                
        pass
get_commuter_flag()             
#%%
# change string number into int
def get_int(num):
    try:
        n=int(num)
    except Exception as e:
        print(e)
        n=''
    return n      
#%%
# cleaning vote data
def parse_Voter(Voter):
    Voter['LexID'] = Voter['LexID'].apply(lambda x: int(x) if type(x)==str else x)
    Voter['Start year']=Voter['Start year'].apply(lambda x:get_int(x))
    Voter['End year']=Voter['End year'].apply(lambda x:get_int(x))
    Voter=Voter.sort_values(by=['Start year','End year']).reset_index(drop=True)
    return Voter

#%%
# add vote address into panel
def get_vote_add(Voter):
    group_v=Voter.groupby('LexID')
    panel_group=panel_df.groupby('LexID')
    for LexID,group in panel_group:
        try:
            voter_unit=group_v.get_group(LexID)
        except Exception as e:
            continue
        for ind1,row1 in voter_unit.iterrows():
            start=voter_unit.loc[ind1,'Start year']
            end=voter_unit.loc[ind1,'End year']
            address=voter_unit.loc[ind1,'Street Address']
            if start=='' or start<2000:
                start==-1
            if end == '' or end <2000:
                end=-1
            if start==-1 and end==-1:
                continue
            for ind2,row2 in group.iterrows():
                year=group.loc[ind2,'Year']
                if year==start or year==end:
                    panel_df.loc[ind2,'voter address']=address
    pass
# create a map between lexID and ExcecID and DirectorID
#%%
#create HQ panel(multiple addresses) 
# lexID and EXECID map
#%%
# get CIK and GVKey map
def get_cik_map():
    cik_map = pd.read_stata('D:/RA Python/CEO/data/gvkey_cik_map.dta')
    cik_map=cik_map[['gvkey','cik']].drop_duplicates()
    cik_map=cik_map[cik_map['cik']!='']
    cik_map=cik_map.rename(columns={"gvkey": 'GVKEY','cik':'CIK'})
    cik_map['CIK']=cik_map['CIK'].apply(lambda x:int(x) if type(x)==str else x)
    return cik_map
cik_map=get_cik_map()
#%%
# extract non-commuter from ExecuCom data
CEO_master=pd.read_excel('D:/RA Python/CEO/Lexis/sample/execu_master_name_DOB_LN_v2.xlsx',usecols = "A:AN")
CEO_master=CEO_master[~CEO_master['LN_ID'].isnull()]
cik_map=get_cik_map()
CEO_master=CEO_master.merge(cik_map,left_on='GVKEY',right_on='gvkey',how="left")
CEO_master['cik']=CEO_master['cik'].apply(lambda x: int(x.split(',')[0]) if type(x)==str else x)
#%%
non_commuter_panel=panel_df[panel_df['Commuter_year_flag'].isnull()]
commuter_panel=panel_df[~panel_df['Commuter_year_flag'].isnull()]
#%%
#get flag all CEO in panel based on year
#Use ExcuCom data to identify tenure period
def get_ceo_cik():
    m_group=CEO_master.groupby('LN_ID')
    panel_group=panel_df.groupby('LexID')
    for LexID,group in panel_group:
        try:
            master_unit=m_group.get_group(LexID)
        except Exception as e:
            continue
        for ind,row in master_unit.iterrows():
            #ExecID=master_unit.loc[ind,'EXECID']
            CIK=master_unit.loc[ind,'cik']
            start=master_unit.loc[ind,'year_beg_CEO']
            end=master_unit.loc[ind,'year_left_ceo']  
            for i,r in group.iterrows():
                year=group.loc[i,'Year']
                if year>=start and year<=end:
                    panel_df.loc[i,'ceo']=1
                    #panel_df.loc[i,'ExecID']=ExecID
                    panel_df.loc[i,'CIK']=CIK
        pass
get_ceo_cik()
#%%
#get a map connecting directID, execID and lexID
def get_ID_map():
    excID_map=CEO_master[['EXECID','LN_ID']].drop_duplicates().rename(columns={"LN_ID": "LexID"})
    excID_map['LexID']=excID_map['LexID'].apply(lambda x: int(x.split(',')[0]) if type(x)==str else x)
    excID_map=excID_map.drop_duplicates()
    commuter_map=master[['LexID','DirectorID','ExecID']].drop_duplicates()
    demo=excID_map.merge(commuter_map,how='outer')
    demo['EXECID']=demo.apply(lambda x: x['ExecID'] if pd.isna(x['EXECID']) else x['EXECID'], axis=1)
    demo.drop('ExecID', axis=1, inplace=True)
    demo=demo.drop_duplicates()
    demo=demo[~(demo.EXECID.isnull()&demo.DirectorID.isnull())]
    demo=demo.sort_values(by=['EXECID','DirectorID']).reset_index(drop=True)
    demo=demo.drop_duplicates(subset='LexID', keep="first")
    return demo
ID_map=get_ID_map()
#%%
# merge to get ExecID and DirectorID for each CEO in panel
# Since these two IDs are unique in ExecuCom and BoardEx
panel_df=panel_df.drop(['ExecID','DirectorID'], axis=1)
panel_df=panel_df.merge(ID_map,how='left')
panel_df=panel_df.reset_index(drop=True)
target=panel_df[(panel_df['ceo'].isnull())&panel_df['Commuter_year_flag'].isnull()]
Direct_panel=target[~target['DirectorID'].isnull()]
Excu_panel=target[target['DirectorID'].isnull()]
Excu_panel=Excu_panel[~Excu_panel['EXECID'].isnull()]
#%%
# Load CIK-GVKey map, ExcuComand boardex data
linking=pd.read_stata('D:/RA Python/CEO/Instrument/Linking_Table.dta')
ceos=pd.read_stata('D:/RA Python/CEO/Instrument/boardex_ceos.dta')
excucomp=pd.read_stata('Execucomp.dta')
#%%
excu=excucomp[['GVKEY','EXECID','BECAMECEO','JOINED_CO','LEFTOFC','LEFTCO','TITLE']].drop_duplicates()
excu['EXECID']=excu['EXECID'].apply(lambda x: int(x) if type(x)==str else x)
excu=excucomp[['GVKEY','EXECID','BECAMECEO','JOINED_CO','LEFTOFC','LEFTCO','TITLE']].drop_duplicates()
excu['EXECID']=excu['EXECID'].apply(lambda x: int(x) if type(x)==str else x)
excu['GVKEY']=excu['GVKEY'].apply(lambda x:int(x) if type(x)==str else x)
excu=excu.merge(cik_map,how='left')
excu['CIK']=excu['CIK'].apply(lambda x:int(x) if type(x)==str else x)
#%%
# Use ExcuCom data to get CEOs previous companies' CIK and CEOs' tiltle in previous companies
def get_excu_cik():
    panel_group=Excu_panel.groupby('EXECID')
    excu_group=excu.groupby('EXECID')
    for EXECID,group in panel_group:
        try:
            excu_unit=excu_group.get_group(EXECID)
        except Exception as e:
            continue
        for ind,row in excu_unit.iterrows():
            CIK=excu_unit.loc[ind,'CIK']
            start=excu_unit.loc[ind,'JOINED_CO']
            if pd.isna(start):
                start=excu_unit.loc[ind,'BECAMECEO']
                if pd.isna(start):
                    continue
            end=excu_unit.loc[ind,'LEFTCO']
            if pd.isna(end):
                end=excu_unit.loc[ind,'LEFTOFC']
                if pd.isna(end):
                    continue  
            start=start.year
            end=end.year
            RoleName=excu_unit.loc[ind,'TITLE']
            for i,r in group.iterrows():
                year=group.loc[i,'Year']
                if year>=start and year<=end:
                    f=0
                    colnm='CIK'
                    while get_col(i,Excu_panel,colnm):
                        colnm='CIK'+str(f)
                        f=f+1
                    col_role=colnm+'Title'
                    Excu_panel.loc[i,col_role]=RoleName
                    Excu_panel.loc[i,colnm]=CIK
        pass
get_excu_cik()
#%%
# create a link between Company ID(BoardEx data) and CIK
def get_board_map():
    board_map=pd.read_stata('D:/RA Python/CEO/Instrument/Linking_Table.dta')
    board_map=pd.read_stata('D:/RA Python/CEO/Instrument/boardex_map.dta')
    board_map=board_map.rename(columns={"companyid": "CompanyID",'gvkey':'GVKEY'})
    board_map['GVKEY']=board_map['GVKEY'].apply(lambda x: int(x) if type(x)==str else x)
    board_map=board_map.merge(cik_map,how='left')
    board_map.drop('GVKEY',axis=1,inplace=True)
    board_map=board_map[~board_map['CIK'].isnull()]
    return board_map
board_map=get_board_map()
#%%
# clean BoardEx data
Board=pd.read_stata('D:/RA Python/CEO/Instrument/BoardEx Emp.dta')
board=Board[['RoleName','DirectorID','CompanyID','DateStartRole','DateEndRole','CompanyName']]
board=board[~board['DateEndRole'].isnull()].drop_duplicates()
board=board[~board['DateStartRole'].isnull()]
board['start']=board['DateStartRole'].apply(lambda x:x.year)
board['end']=board['DateEndRole'].apply(lambda x:x.year)
board=board[board['end']>1999]
board=board.merge(board_map,how='left')
board['CIK']=board['CIK'].apply(lambda x:int(x) if type(x)==str else x)
#%%
# boolean method to judge if this cell is empty
def get_col(i,Direct_panel,colnm):
    try:
        result=(not pd.isna(Direct_panel.loc[i,colnm]))
    except:
        result=False
    return result 
#%%
panel_df['DirectorID']=panel_df['DirectorID'].apply(lambda x:int(x) if type(x)==str else x)
board['DirectorID']=board['DirectorID'].apply(lambda x:int(x) if type(x)==str else x)
Direct_panel['DirectorID']=Direct_panel['DirectorID'].apply(lambda x:int(x) if type(x)==str else x)
#%%
# Get CIK from BoardEx
#for every company CEO worked before, generate a new column showing CIK and corresponding job title
def get_board_cik():
    panel_group=Direct_panel.groupby('DirectorID')
    demo=board[~board['CIK'].isnull()]
    b_group=demo.groupby('DirectorID')
    
    for DirectorID,group in panel_group:
        try:
            b_unit=b_group.get_group(DirectorID)
        except Exception as e:
            continue

        for ind,row in b_unit.iterrows():
            CIK=b_unit.loc[ind,'CIK']
            start=b_unit.loc[ind,'start']
            end=b_unit.loc[ind,'end']
            RoleName=b_unit.loc[ind,'RoleName']
            for i,r in group.iterrows():
                year=group.loc[i,'Year']
                if year>=start and year<=end:
                    f=0
                    colnm='CIK'
                    while get_col(i,Direct_panel,colnm):
                        colnm='CIK'+str(f)
                        f=f+1
                    col_role=colnm+'Title'
                    Direct_panel.loc[i,col_role]=RoleName
                    Direct_panel.loc[i,colnm]=CIK
        pass
#%%
# load HQ address crawled from SEC
HQ_add=pd.read_excel('D:/RA Python/CEO/HQ_add2.xlsx')
HQ_add=HQ_add[['CIK','report_yr','correct_add']]
HQ_add=HQ_add.drop_duplicates(subset=['CIK','report_yr'],keep='first')
#%%
# left join panel with HQ address to get HQ address panel for CEOs
target=pd.concat([Direct_panel,Excu_panel])
final=pd.concat([target,panel_df.loc[~panel_df.index.isin(target.index)]])
final['CIKTitle']=final.apply(lambda x: 'CEO' if x['ceo']==1 or x['Commuter_year_flag']==1 else x['CIKTitle'], axis=1)
final=panel_df.merge(HQ_add,how='left', left_on=['CIK','Year'], right_on = ['CIK','report_yr'])
#%%
# function to select HQ address based on CIK and time
def get_add(HQ_add,CIK,year):
    subset=HQ_add[(HQ_add['CIK']==CIK) & (HQ_add['report_yr']==year)].head(1)
    try:
        add=subset['correct_add'].item()
    except:
        add=np.nan
    return add
#%%
# get HQ addresses for companies that CEOs worked before
def merge_HQ(final,HQ_add,colnm):
    result=final.merge(HQ_add,how='left', left_on=[colnm,'Year'], right_on = ['CIK','report_yr'])
    new_col=colnm+'_add'
    result=result.rename(columns={'correct_add':new_col})
    result=result.drop('report_yr',axis=1)
    return result
#%%
# iterate all column contains 'CIK' to merge HQ address
def merge_add():
    result=merge_HQ(final,HQ_add,'CIK')
    for i in range(0,10):
        colnm='CIK'+str(i)
        result=merge_HQ(result,HQ_add,colnm)
    return result
result=merge_add()

#%%
CEO.drop('EXECID',axis=1,inplace=True)
final=result.merge(CEO,how='left')
final=final.replace('\\n',' ')
final.to_csv('/instrument_result/panel2.csv',sep='|')
