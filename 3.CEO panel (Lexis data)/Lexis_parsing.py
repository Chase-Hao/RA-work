# -*- coding: utf-8 -*-
"""
Created on Wed Jun 16 23:13:22 2021

Iterate all Lexis report files
Pasrse them into dataframe

@author: haoch
"""
#%%
import os
import re
from dateutil import parser
import pandas as pd
import datetime
import json
#%%
# Iterate all lexis files
def main(og_path):
    err_list=[] 
    tot_list=[]
    for file in os.listdir(og_path):
        if file.endswith(".txt"):
            data_file=os.path.join(og_path, file)
            f = open(data_file, encoding="utf-8").read()
            try:
                CEO=get_CEO_info(f)
                try:
                    vote=get_vote_list(f)
                except Exception as e:
                    print(str(e))
                    print(data_file)
                    vote=[]
                spouse=get_spouse_list(f)
                tot_list.append({"CEO":CEO,"vote":vote,"spouse":spouse})
                #tot_list.append({"CEO":CEO,"vote":[],"spouse":spouse})
            except Exception as e:
                print(str(e))
                #print(data_file)
                err_list.append(data_file)
    return tot_list,err_list

#%%
# The function to generate dataframe for lexis data
def get_df(f):
    df1=[]
    for unit in f:
        CEO=unit['CEO']
        Full_Name=CEO['Full Name']
        LexID=CEO['LexID']
        SSN=CEO['SSN']
        SSN_State=CEO['SSN State']
        Add_list=CEO['Add_list']
        spouse=unit['spouse']
        DOB=CEO['DOB']
        if len(spouse)==0:
            Spouse_DOB=''
            Spouse_full_name= ''
            Spouse_SSN= ''
            Spouse_add_list = ''
        else:
            Spouse_DOB=spouse['Spouse DOB']
            Spouse_full_name=spouse['Spouse full name']
            Spouse_SSN=spouse['Spouse SSN']
            Spouse_add_list =spouse['Spouse_add_list']
        vote=unit['vote']
# use dict to contain all Lexis Data        
        for add in Add_list:
            add_unit={"Address":"Home","Full Name":Full_Name,"LexID":LexID,'SSN':SSN,'SSN State':SSN_State,"DOB":DOB,\
                      "Spouse full name":Spouse_full_name,"Spouse SSN":Spouse_SSN,"Spouse DOB": Spouse_DOB,\
                          # 'city':add}
                           'Street Address':add['Street Address'],"Start month":add['Start month'],\
                               "Start year":add["Start year"],"End month":add["End month"],\
                               "End year":add["End year"],"Household Members":add["Household Members"],\
                                   'house_value':add["house_value"]}
                
            df1.append(add_unit)
    
        for add in Spouse_add_list:
            add_unit={"Address":"Spouse","Full Name":Full_Name,"LexID":LexID,'SSN':SSN,'SSN State':SSN_State,"DOB":DOB,\
                      "Spouse full name":Spouse_full_name,"Spouse SSN":Spouse_SSN,"Spouse DOB": Spouse_DOB,\
                      'Street Address':add}
            df1.append(add_unit)
        for add in vote:
            add_unit={"Address":"Voter","Full Name":Full_Name,"LexID":LexID,'SSN':SSN,'SSN State':SSN_State,"DOB":DOB,\
                      "Spouse full name":Spouse_full_name,"Spouse SSN":Spouse_SSN,"Spouse DOB": Spouse_DOB,\
                          'Street Address':add['Street Address'],"Start month":add['Start month'],\
                          "Start year":add["Start year"],"End month":add["End month"],"End date":add["End date"],\
                              "End year":add["End year"],"Party":add["Party"]}
            df1.append(add_unit)
    return df1
# remove unrelated words in address
def clean(dataf):
    dataf['Street Address'] = \
        dataf['Street Address'].apply(lambda x:\
                                      re.sub('\\(?\d{3}\D{0,3}\d{3}\D{0,2}\d{4}', '', x)) 
    dataf['Street Address'] = dataf['Street Address'].apply(lambda x:x.replace("Address/Phone,",'').strip())
    dataf['Street Address']=dataf['Street Address'].apply(lambda x:clean_add(x))
    dataf=dataf.replace('\n','')
    pass
# derive the birth state of spouse
def get_spouse_ssn(CEO_add):
    SSN_map=get_ssn_map()
    CEO_add['num'] = CEO_add['Spouse SSN']\
        .apply(lambda x: int(x.split('-')[0]) if len(str(x).split('-'))>=2 else x)
    CEO_add['Spouse_SSN_state']=CEO_add['num'].apply(lambda x:SSN_map.get(x) if str(x).isdigit() else x)
    CEO_add.drop('num', axis=1, inplace=True)
    pass

# Some CEOs missing birth state, which can be gotten from SSN
def get_miss_state(CEO_add):
    SSN_map=get_ssn_map()
    CEO_add['num'] = CEO_add['SSN']\
        .apply(lambda x: int(x.split('-')[0]) if len(str(x).split('-'))>=2 else x)
    CEO_add['SSN State']=CEO_add.apply(lambda x:SSN_map.get(x['num']) \
                                       if str(x['num']).isdigit() and\
                                       (x['SSN State']=='' or str(x['SSN State'])=='None') else x['SSN State'], axis=1)
    CEO_add.drop('num', axis=1,inplace=True)
    pass

#%%
# Get CEO name, LexID, DOB,SSN, SSN state and House address info
def get_CEO_info(f):
    full_name=find_nextn(f,f.find('Full Name'),1)
    #address=find_nextn(f,f.find('Address'),2)
    lexID=find_nextn(f,f.find('LexID'),1)
    DOB=find_nextn(f,f.find('DOB'),1)
    SSN=find_nextn(f,f.find('SSN'),1)
    if SSN=='':
        SSN=re_search("SSN:(.*?)(   |\n)",f)
    ssn_state=get_ssn_state(f)
    #add_list=find_add_list(f)
    add_list=get_CEO_city(f)
    DOB=find_nextn(f,f.find('DOB'),1)
    unit={'Full Name':full_name,"SSN State":ssn_state,'SSN':SSN,"LexID":lexID,"DOB":DOB,"Add_list":add_list}
    return unit

def next_line(f,ind_tar):
    n1=f.rfind('\n',0,ind_tar)
    length_tar=ind_tar-n1
    n2=f.find('\n',ind_tar)
    n3=f.find('\n',n2+1) 
    
    if n3-n2<length_tar:
        return '',''
    else:
        ind_begin=n2+length_tar    
        ind_end1=f.find('  ',ind_begin)
        ind_end2=f.find('\n',ind_begin) 
        if ind_end1==-1:
            return ind_begin,ind_end2
        else:
            return ind_begin,ind_end1
#get the next line of targeted string 
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
# get CEO ssn state
def get_ssn_state(f):
    ind_tar=f.find('SSNs Summary')
    if ind_tar==-1:
        return ''
    ind_tar=f.find('State',ind_tar)
    b,e=next_line(f,ind_tar)
    b,e=next_line(f,b)
    return f[b:e]
# get address information including address, household members, value, start and end time
def find_add_list(f):
    add_list=[]
    ind_start=f.find('Address Details')
    #ind_end=f.find("Voter Registrations")
    ind_end=f.find("records found",ind_start)
    if ind_start>ind_end or ind_start==-1 or ind_end==-1:
        return add_list
    flist=re.split('\\n\d*?:', f[ind_start:ind_end])
    for f1 in flist:
        if len(f1)>50:
            add=re.search("^(.*?)\n",f1).group(1)
            date=find_nextn(f1,f1.find('Dates'),1)
            mem=re_search("Household Members((.|\n)*?)Other Associates",f1)
            mem=re.sub("\n+", ";",mem).strip(";")
            if 'None Listed'in mem:
                mem=''
            start_mon,start_year,end_mon,end_year =cut_date(date)
            house_value=re_search("Median Home Value: (.*?)\\n",f1).replace('$','').replace(',','')
            add_unit={'Street Address':add,"Start month":start_mon,\
                      "Start year":start_year,"End month":end_mon,\
                          "End year":end_year,"Household Members":mem,\
                              'house_value':house_value}
            add_list.append(add_unit)

    return add_list
# clean date, extract year and month for different date format
def cut_date(date):
    date=date.strip()
    if len(date)>21:
        return '','','',''
    if len(date)<4:
        return '','','',''
    d_list=date.split('-')
    if len(d_list)==1:
        start=d_list[0]
        start_mon,start_year=cut_mon_year(start)
    if len(d_list)==2:
        start=d_list[0]
        start_mon,start_year=cut_mon_year(start)
        end=d_list[1]
        end_mon,end_year=cut_mon_year(end)
    return start_mon,start_year,end_mon,end_year

# split date into year and month
def cut_mon_year(start):
    start=start.strip().split('/')   
    if len(start[0])>3:
        start_mon=''
        start_year=start[0]
    else:
        start_mon=start[0]
        start_year=start[1]  
    return start_mon,start_year

#get all vote records information
def get_vote_list(f):
    vote_list=[]
    try:
        r=re.search("Voter Registrations -(.*?)\s*records found",f)
    except:
        return vote_list
    num=int(r.group(1))
    if num==0:
       return vote_list
    ind_start=r.end() 
    ind_end=f.find('records found',ind_start)
    flist=re.split('\\n\d*?:', f[ind_start:ind_end])
    for f1 in flist:
        if len(f1)>50:
            vote_list.append(get_vote_add(f1))
    return vote_list

# get section like 'voter', 'boat','potential relatives'
def get_section(up_p,down_p,ind_start,f):
    r_up=re.search(up_p,f[ind_start:len(f)])
    ind_up=r_up.start()+ind_start
    num_up=int(r_up.group(1))
    r_down=re.search(down_p,f[ind_up:len(f)])
    ind_down=r_down.start()+ind_up
    num_down=int(r_down.group(1))
    return ind_up,num_up,ind_down,num_down

# get vote address info like registration date, address,party, last vote date
def get_vote_add(f1):
    re_add=re_search("Residential Address: (.*?\\n.*?)\\n",f1)
    add=re_add.replace("\n",',').replace("  ","")
    regist_mon,regist_year=vote_date("Registration Date:(.*?)\\n",f1)
    last_mon,last_year=vote_date("Last Vote Date:(.*?)\\n",f1)
    last_date=get_date_num(last_mon,last_year)
    party=get_vote_info("Party Affiliation:(.*?)\\n",f1)
    vote_add={'Street Address':add,"Start month":regist_mon,"Start year":regist_year,\
             "End month":last_mon,"End year":last_year,"Party":party,"End date":last_date}
    return vote_add

# transform date format into a number so that I can filter out date before 2000
def get_date_num(month,year):
    if year=='':
        return 0
    if month=='':
        month=0
    month='{:02}'.format(int(month))
    return int(str(year)+month)        

#get registration date
def get_regist_date(pattern,f1):
    re_regist_date=re.search(pattern,f1)
    if re_regist_date==None:
        regist_date=''
    else:
        regist_date=re_regist_date.group(1)
    return regist_date

# devide vote date into year and month for different date format
def vote_date(pattern,f1):
    regist_date=get_regist_date(pattern,f1)
    try:
        date=parser.parse(regist_date)
        return date.month,date.year
    except:
        return '',''
    
#%%
#get spouse info
def get_spouse_list(f):
    r=re.search("Potential Relatives -(.*?)records found",f)
    num=int(r.group(1))
    if num==0:
        return {}
    ind_start=r.end()
    f1=get_spouse_section(ind_start,f).replace("\n\n",'\n',1)
    spouse_name=find_nextn(f1,f1.find('Full Name'),1)
    spouse_ssn=re_search("SSN:(.*?)(   |\n)",f1)
    spouse_dob=re_search("DOB:(.*?)(   |\n)",f1)
    spouse_add_list=spouse_add(f1)
    spouse_unit={"Spouse full name":spouse_name,'Spouse_add_list':spouse_add_list,\
                 "Spouse SSN":spouse_ssn,"Spouse DOB":spouse_dob}
    return spouse_unit

# get content only including pontential relatives
def get_spouse_section(ind_start,f):
    ind_start=f.find('No.',ind_start)
    p="\\n\d\."
    r_up=re.search(p,f[ind_start:len(f)])
    ind_up=r_up.end()+ind_start
    try:
        r_down=re.search(p,f[ind_up:len(f)])
        ind_down=r_down.start()+ind_up
    except:
        try:
            p="\\n\d:"
            r_down=re.search(p,f[ind_up:len(f)])
            ind_down=r_down.start()+ind_up
        except:
            ind_down=f.find('Business Associates',ind_start)
    return f[ind_start:ind_down]

# get spouse addresses
def spouse_add(f1):
    flist=f1.split('\n')
    ind_start=flist[0].find("Address")
    add_list=[]
    add=''
    for l in flist:
        if len(l)>ind_start:
            if add=='':
                add=l[ind_start:len(l)]
            else:
                add=add+", "+l[ind_start:len(l)]
        else:
            if len(add)>10:
                add_list.append(add)
            add=''
    return add_list

# rewrite search function make sure it return empty when it doesn't find anything
def re_search(pattern,f1):
    try:
        return re.search(pattern,f1).group(1)
    except:
        return ''

# transform different date format into a general one
def parse_spouse_DOB(Commute):
    Commute['Spouse DOB'] = Commute['Spouse DOB']\
        .apply(lambda x: parser.parse(str(x),default=datetime.datetime(1000, 1, 1)).date()\
               if str(x)!='nan' and x !='' \
                   else x\
              )
    pass

def parse_DOB(Commute):
    Commute['DOB'] = Commute['DOB']\
        .apply(lambda x: parser.parse(str(x),default=datetime.datetime(1000, 1, 1)).date()\
               if str(x)!='nan' and x !='' \
                   else x\
              )
    pass

def find_first_name(name):
    first=name.split(',')[1]
    nm_list = first.split()[0]
    #nm_list = [part.strip() for part in nm_list if len(part)>2 and part!='III']
    #first=' '.join(nm_list)  
    return nm_list

#%%
# get CEO address city from address summary part
# Since in this part, address is sperated by ','
def get_CEO_city(f):
    add_list=[]
    ind_start=f.find('Address Summary')
    ind_start=f.find('No.',ind_start)
    #ind_end=f.find("Voter Registrations")
    ind_end=f.find("Address Details",ind_start)
    if ind_start>ind_end or ind_start==-1 or ind_end==-1:
        return add_list
    flist=re.split('\\n\d*?:', f[ind_start:ind_end])
    for f1 in flist:
        if len(f1)>50:
            add=re.search("\n(.*?)\n",f1).group(1)
            city=add.split(',')[0].strip()
            add_list.append(city)
    return list(set(add_list)) 
#%%
# find SSN link with state and transform it into dict format
def get_ssn_map():
    file = open("Lexis\\SSN.txt", "r")
    #SSN_list = []
    SSN_list={}
    for line in file:
        stripped_line = line.strip()
        line_list = stripped_line.split(':')
        state=line_list[1].strip()
        number_list=line_list[0].strip().split('-')
        if len(number_list)==1:
            num=int(number_list[0])
            SSN_list[num] = state
            #N_list.append({'num':num,'state':state})
        else:
            start=int(number_list[0])
            end=int(number_list[1])+1
            for num in range(start,end):
                SSN_list[num] = state
                #SSN_list.append({'num':num,'state':state})
    #SSN_map=pd.DataFrame(SSN_list)
    return SSN_list

#%%
f_1,err_list=main("D:\\RA Python\\CEO\\Lexis\\Lexis Reports")
All=pd.DataFrame(get_df(f_1))
#%%
# Clean data
def cl_df(All):
    All['LexID']=All['LexID'].apply(lambda x: int(x) if str(x).isdigit() else x)
    All['DOB']=All['DOB'].apply(lambda x: x.replace('WESTCHESTER COUNTY','5/1984')\
                                if type(x)==str else x) 
    All['CEO last name']=All['Full Name'].apply(lambda x:x.split(',')[0] if len(str(x))>3 else x)
    All['CEO first name']=All['Full Name'].apply(lambda x:find_first_name(x) if len(str(x))>3 else x)
    All['Spouse last name']=All['Spouse full name'].apply(lambda x:x.split(',')[0] if len(str(x))>3 else x)
    All['Spouse first name']=All['Spouse full name'].apply(lambda x:find_first_name(x) if len(str(x))>3 else x)
    clean(All)
    parse_spouse_DOB(All)
    parse_DOB(All)
    get_miss_state(All)
    get_spouse_ssn(All)
    pass

# get state of every address
def find_street_state(add):
    state_map=json.load(open('D:/RA Python/Distance Test1/states_hash.json',))
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

# remove unrelated words in address
def clean_add(add):
    add_list = add.split(',')
    add_list = [part.strip() for part in add_list if len(part)>1 and 'COUNTY' not in part]
    add=', '.join(add_list)
    return add

# get zip code for each address
def get_zip(add):
    add_zip = add.split(',')[-1].split('\xa0')[-1].split('-')[0]
    if not add_zip.isdigit():
        add_zip = add.split(',')[-1].split(' ')[-1].split('-')[0]
        if not add_zip.isdigit():
            add_zip = ''
    return add_zip
#%%
# conduct cleaning function and find name and zip function
cl_df(All)
All['CEO last name']=All['Full Name'].apply(lambda x:x.split(',')[0] if len(str(x))>3 else x)
All['CEO first name']=All['Full Name'].apply(lambda x:find_first_name(x) if len(str(x))>3 else x)
All['Street Address state']=All['Street Address'].apply(lambda x:find_street_state(x) if len(str(x))>3 else x)
All['Zip']=All['Street Address'].apply(lambda x:get_zip(x) if type(x)==str else x)
