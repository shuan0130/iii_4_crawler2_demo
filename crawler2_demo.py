
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from bs4 import BeautifulSoup
import csv
import threading
import time
import re
from math import cos
import numpy as np
from trans_state import state_code_compute
from lat_lon import geoc
from kafka_push import kafka_push


# In[2]:


def junior_crawler():
    junior = requests.get("https://zh.wikipedia.org/wiki/%E8%87%BA%E5%8C%97%E5%B8%82%E5%9C%8B%E6%B0%91%E4%B8%AD%E5%AD%B8%E5%88%97%E8%A1%A8")
    junior_text = pd.read_html(junior.text)
    try:
        for jun_df in junior_text :
            jun_df = pd.concat(junior_text[1:13],ignore_index=True)
        jun_df[2] = jun_df[2].str.replace('\[.*\]','')
        jun_df = jun_df.drop([4],axis=1)
        jun_df = jun_df.drop_duplicates()
        jun_df = jun_df.drop([0,3],axis=1)
        jun_df.columns = ['校名','地址']
        jun_df = jun_df.drop([0])
        jun_df = jun_df.reset_index(drop=True)
    except:
        print('datacleaner error')
    jundf = geoc(jun_df,1)
    new_col = jun_df.apply(state_code_compute,axis=1)
    jun_df['state_code'] = new_col
    kafka_push(jun_df,9092,'test')


# In[3]:


def highsch_crawler():
    high_r = requests.get("https://zh.wikipedia.org/wiki/%E8%87%BA%E5%8C%97%E5%B8%82%E9%AB%98%E7%B4%9A%E4%B8%AD%E7%AD%89%E5%AD%B8%E6%A0%A1%E5%88%97%E8%A1%A8")
    high_text = pd.read_html(high_r.text)
    try:
        for highdf in high_text :
            highdf = pd.concat(high_text[0:12],ignore_index=True)
        highdf.drop_duplicates(inplace=True)
        highdf[2] = highdf[2].str.replace('\[.*\]','')
        highdf.drop([0,3,4],axis=1,inplace=True)
        highdf.drop([0],inplace=True) # 刪列
        highdf.columns = ['校名','地址']
        highdf = highdf.reset_index(drop=True)    
    except:
        print('datacleaner error')
    highdf = geoc(highdf,1)
    new_col = highdf.apply(state_code_compute,axis=1)
    highdf['state_code'] = new_col
    kafka_push(highdf,9092,'test')


# In[4]:


def elemen_crawler():
    ele_r = requests.get("https://zh.wikipedia.org/wiki/%E8%87%BA%E5%8C%97%E5%B8%82%E5%9C%8B%E6%B0%91%E5%B0%8F%E5%AD%B8%E5%88%97%E8%A1%A8")
    ele_tb = pd.read_html(ele_r.text)
    for eledf in ele_tb :
        eledf = pd.concat(ele_tb[0:12],ignore_index=True)
    eledf = eledf.drop([2],axis=1)
    eledf.columns = ['校名','地址']
    eledf = eledf.drop_duplicates()
    eledf = eledf.drop([0])
    eledf = eledf.reset_index(drop=True) 
    eledf = geoc(eledf,3)
    new_col = eledf.apply(state_code_compute,axis=1)
    eledf['state_code'] = new_col
    kafka_push(eledf,9092,'test')


# In[5]:


t1 = threading.Thread(target=junior_crawler)
t2 = threading.Thread(target=highsch_crawler)
t3 = threading.Thread(target=elemen_crawler)
t1.start()
t2.start()
t3.start()
t1.join()
t2.join()
t3.join()
print('done')

