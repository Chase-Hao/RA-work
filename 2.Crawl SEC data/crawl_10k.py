# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 12:02:55 2021

This code is to crawl HQ address from 10k financial reports

@author: haoch

"""
import re
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import difflib

# %%
# Regex to extract US street address
class Pattern_add:
    street1 = "Avenue|Way|Lane|Road|Boulevard|port|Drive|Street|Beach|Place|Box|pike|shore|Building|way|Row|Tower|Suite|Plaza|Square|sq|Trail|trl"
    street2 = "Ave|Dr|Rd|Blvd|Ln|St|hwy"
    street=street1+"|"+street1.upper()+"|"+street2
    state1 = "Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New.?Hampshire|New.?Jersey|New.?Mexico|New.?York|North.?Carolina|North.?Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode.?Island|South.?Carolina|South.?Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West.?Virginia|Wisconsin|Wyoming"
    state_abb="AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MT|NE|NV|N\.?H|N\.?J|N\.?M|N\.?Y|N\.?C|N\.?D|MP|OH|OK|OR|PW|PA|PR|R\.?I|S\.?C|S\.?D|TN|TX|UT|VT|VI|VA|WA|W\.?V|WI|WY"
    state=state1+"|"+state1.upper()
    num1='One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|P\.?O'
    num=num1+"|"+num1.upper()
    p_full="((\d{1,5}|"+num+")([^0-9\:\\[\\(\\)\|]{1,20})("+street+")([^\:\\(\\)\|]{1,30})("+state+"))"
    p_abb="((\d{1,5}|"+num+")([^0-9\:\\[\\(\\)\|]{1,20})("+street+")([^\:\\(\\)\|]{1,30})("+state_abb+"))"

#%%
#searching for keywords in content
def find_relate(key1, key2, text):
    i = text.find(key1)
    key = ''
    while i != -1:
        for k in key2:
            if i-600 > 0:
                j = text.find(k, i-300, i+300)
            else:
                j = text.find(k, 0, i+600)
            if j != -1:
                key = key+k+","
                key2.remove(k)
                continue
        i = text.find(key1, i+1)
    return key

# %%
# Iterate url to get financial reports
# get indicator by searching keywords
# cut the useful part which contains HQ reports
def new_spider(test):
    for ind, row in test.iterrows():
        try:
            url = test.loc[ind, 'url'].replace('-index.htm', '.txt')
            html = str(requests.get(url).content)
            b = 0
            ind1 = html.find('</SEC-HEADER>')
            #company=test[ind,'Company'].split(' ')[0].lower().strip()
            while ind1 == -1 and b < 4:
                b = b+1
                html = str(requests.get(url).content)
                ind1 = html.find('</SEC-HEADER>')
                time.sleep(0.1)
            if ind1 != -1:
                key = find_relate(
                    'relocat', ['headquart', 'office', 'operat'], html.lower())
                test.loc[ind, 'key'] = key
                ind2 = html.lower().find('</ix:header>', ind1)
                ind3 = html.lower().find('indicate by', ind1)
                if ind2 != -1 and ind2<ind3:
                    test.loc[ind, 'con_add'] = html[ind2:ind3]
                if ind2>ind3:
                    test.loc[ind, 'con_add'] = html[ind1:ind3]
                else:
                    test.loc[ind, 'con_add'] = html[ind1:ind3]
        except:
            print(ind, test.loc[ind, 'url'])
            continue
    pass

# %%
# Search for HQ address
def lam_add(var):
    my = find_add(var)
    my1 = add_parse1(my)
    my2 = rm_begin(my1)
    return my2

#
def step1(test):
    for ind, row in test.iterrows():
        con_add = test.loc[ind, 'con_add']
        var1 = get_var(con_add)
        test.loc[ind, 'var1'] = var1
        my = find_add(var1)
        my1 = add_parse1(my)
        my2 = rm_begin(my1)
        test.loc[ind, '10k_add'] = my2
    pass

# clean the useful part of html
def get_var(con_add):
    soup = BeautifulSoup(con_add, 'html.parser')
    var1 = soup.get_text()
    #remove html tags
    p = re.compile(r'<\s*((?:.|\n)*?)>')
    var1 = p.sub('', str(var1))
    #var1=re.sub('(\\\\n *)+', '|',var1)
    var1 = re.sub('(\\\\t)+', '\\t', var1)
    var1 = re.sub('(\\\\n\s*)+', ',', var1)
    #var1=re.sub('(\| *)+', '|',var1)
    var1 = re.sub('(\\t)+', ' ', var1)
    var1 = re.sub('( |_|-|=)+', ' ', var1)
    return var1

# Locate address based on key word '(address of Principle)'or'address of'
def find_add(var1):
    i1 = var1.lower().find('(add')
    address ='no address'
    if i1 == -1:
        i1 = var1.lower().find('address of')
        if i1 == -1:
            # (Principal
            return address
    if i1 != -1:
        i2 = var1.rfind(')', 0, i1)
        if i2 == -1:
            print('no )')
            return address
        else:
            address = var1[i2+1:i1].strip()
            while i1-i2 <24 and i2 != -1:
                address = var1[i2+1:i1].strip()
                i2 = var1.rfind(')', 0, i2)
            if i2 != -1:
                address = var1[i2+1:i1].strip()
            return address

# remove unrelated words like zip-code, telephone
def add_parse1(my):
    my = my.replace('Not Applicable', '')
    my=my.replace('Telephone', '')
    #my = re.sub('\s\s+', ',', my)
    my = re.sub('(,\s*)+', ',', my)
    my = re.sub(',+', ',', my).strip(',')
    # remove (zip code)
    my = re.sub('\\(?\d{3}\D{0,3}\d{3}\D{0,2}\d{4}', '', my)
    my=re.sub("\\(.*?\\)", '', my)
    # remove telephone number
    return my

#remove short unrelated words like 'incorporate', 'address'
def rm_begin(my):
    mylist = my.split(',')
    mylist = [x for x in mylist if x.strip()]
    for i in range(len(mylist)):
        st = mylist[i]
        if any(char.isdigit() for char in st) or re.search(Pattern_add.street,st)!=None:
            after=",".join(mylist[i:len(mylist)])
            if len(after)<30:
                return my
            else:
                return after
    return my 

#The second way using Regex to locate address and get raw result 
#This mothod used regex sepratedly
#can only get one address
def trim_long(my):
    p=Pattern_add
    if re.search(p.state,my)==None and re.search(p.state_abb,my)==None:
        return my
    else:
        #find state first
        #return more than one state
        if re.search(p.state,my)!=None:
            sta,s1,end=re_last(Pattern_add.state,my)
        if re.search(p.state,my)==None:
            sta,s1,end=re_last(p.state_abb,my)
        # find the first word of street address
        if re.search(p.street,my[0:s1])!=None:
            strt,st1,st2=re_last(p.street,my[0:s1])
            if re.search("\d+",my[0:st1])!=None:
                number,n1,n2=re_last("\d+",my[0:st1])
                return my[n1:end]
            if re.search(p.num,my[0:st1])!=None:
                number,n1,n2=re_last(p.num,my[0:st1])
                return my[n1:end]
            else:
                return my
                
        else:
            return my

# Use regex to locate the last word
def re_last(pattern,my):
    last_state=[(m.group(),m.start(),m.end()) for m in re.finditer(pattern,my)][-1]
    place=last_state[0]
    start=last_state[1]
    end=last_state[2]
    return place,start,end   

p1=Pattern_add.p_full
p2=Pattern_add.p_abb

# This method is using full street regex
# It can only extract address complying regex form
def reg_long(add):
    global p1,p2
    if len(re.findall(p1,add))==1:
        after=re.findall(p1,add)[0][0]
        return after
    else:
        if len(re.findall(p2,add))==1:
            after=re.findall(p2,add)[0][0]
            return after
        else:
            return add
# For no address and wrong subset
def no_add(var1):
    star=var1.lower().find('file')
    new_add=reg_long(var1[star:len(var1)])
    if len(new_add)>120:
        return 'no address'
    else:
        return new_add

# find another start point to elminate noise
# because address regex can capture strings in the header of financial reports
    star=var1.lower().find('file')
    if re.findall(p1,var1[star:len(var1)])!=[]:
        new_add=re.findall(p1,var1[star:len(var1)])[0][0]
        return new_add
    else:
        if re.findall(p2,var1[star:len(var1)])!=[]:
            new_add=re.findall(p2,var1[star:len(var1)])[0][0]
            return new_add
        else:
            return 'no address'
   
# If there are multiple companies
# Search company name first. HQ address usually under company name
def mul_com1(var,com):
    cname=get_com(com)
    if re.search(cname,var)==None:
        return 'no address'
    begin=re.search(cname,var).start()
    var=var[begin:len(var)]
    if len(re.findall(p1,var))>0:
        add=re.findall(p1,var)[0][0]
    else:
        add='no address'
    return add   
# get company name
def get_com(com):
    coml=com.split(' ')
    if len(coml)<=2:
        name=coml[0]
    if len(coml)>2:
        coml=coml[0:2]
        name='.?'.join(coml)
    return name

#%%
# use different method for wrong parsed result
# if length of result >90 or <25, try different way to parse until
def final_step(test1):
    for ind,row in test1.iterrows():
        #var=test1.loc[ind, 'var1']
        my=test1.loc[ind, '10k_add']

        if len(my)>90 or len(my)<25:
            test1.loc[ind, 'not_sure']=1
        else:
            test1.loc[ind, 'not_sure']=0
            my=no_add(var)
        if len(my)>90 or len(my)<25:
            com=test1.loc[ind, 'Company']
            my=mul_com1(var,com)
        if len(my)>90 or len(my)<25:
            my=last_no(var)
        if len(my)>90 or len(my)<25:
            my=test1.loc[ind, '10k_add']
        if my==test1.loc[ind, '10k_add']:
            continue
        test1.loc[ind, '10k_add']=my
    pass
#%%
final_step(demo)
#%%  
demo=demo.sort_values(by=['CIK','report_time'])#.reset_index(drop=True)     
grouped=demo.groupby('CIK')
#%%
# remove float np.nan
def change_na(add):
    add=str(add)
    if len(add)<=3:
        add=''
    return add
#%%
# get index for HQ relocation based on parsed company HQ address
# Using 80% similarity to identify change of location
demo=demo.sort_values(by=['CIK','report_time'])#.reset_index(drop=True)     
grouped=demo.groupby('CIK')
for group, df in grouped:
    ind0=df.index[0]
    add0=df.loc[ind0,'10k_address']
    add0=change_na(add0)
    for ind, row in df.iterrows():
        add=df.loc[ind,'10k_address']
        add=change_na(add)
        ratio=similar(add0.lower(),add.lower())
        demo.loc[ind,'last_year_this_year_similarity']=ratio
        if ratio<0.8:
            demo.loc[ind,'last_this_diff_add_flag(ratio<0.8)']=1
            demo.loc[ind0,'last_this_diff_add_flag(ratio<0.8)']=2
        else:
            demo.loc[ind,'last_this_diff_add_flag(ratio<0.8)']=0
        ind0=ind
        add0=add

#%%
#get similarity
def similar(s1, s2):
    return difflib.SequenceMatcher(lambda x: x in ",\t ()", s1, s2).quick_ratio()
#%% Pattern_add.state
#remove companies whose have abroad HQ
def US_flag(state):
    if str(state)=='nan':
        return 'empty'
    state=state.upper()
    if re.search(Pattern_add.state_abb,state)!=None:
        return 1
    else:
        return 0
demo['US_flag']=demo['STATE'].apply(lambda x:US_flag(x))
#%%
demo_US=demo[demo['US_flag']!=0]
demo_US=demo_US.drop(['US_flag'],axis=1)
demo_US=demo_US.fillna('')
demo_US=demo_US.sort_values(by=['CIK','report_time']).reset_index(drop=True) 
#%%
demo_US['10k_add']=demo_US['10k_add'].apply(lambda x:x.replace('\n',''))
demo_US.to_csv('D:/RA Python/CES data/data/10-K Address_US.csv',sep='|')