
# coding: utf-8

# In[1]:


import requests
import pymssql
from lxml import etree
import json
import pandas as pd

class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        
    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur


# In[2]:

url = "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=%E7%94%B5%E5%BD%B1&start=0&genres=%E5%89%A7%E6%83%85&countries=%E7%BE%8E%E5%9B%BD"
headers = {'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"}
data = requests.get(url,headers = headers).text


# In[3]:

dicts = json.loads(data)
dicts


# In[4]:

df = pd.DataFrame(dicts["data"])
df.head()


# In[5]:

df.drop("cover_x",axis = 1,inplace = True)
df.drop("cover_y",axis = 1,inplace = True)
df.head()


# In[11]:

import requests
from lxml import etree
import json
import pandas as pd
import pymssql
#encoding=utf-8


# In[12]:

headers = {'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"}


# In[15]:

for i in range(0,480,20):
    url = "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=%E7%94%B5%E5%BD%B1&start={N}&genres=%E5%89%A7%E6%83%85&countries=%E7%BE%8E%E5%9B%BD".format(N = i)
    data = requests.get(url,headers = headers).text
    
    dicts = json.loads(data)
    
    #print (dicts)
    for j in range (0,20):
        movie1=dicts['data'][j]
        print(movie1)
        directors=movie1['directors'][0]
        rate=movie1['rate']
        star=movie1['star']
        title=movie1['title']
        casts=movie1['casts'][0]
        url=movie1['url']
        print()
        db=pymssql.connect(server=".",user="sa",password="123456",database="caijunai",charset='utf8')
        cursor=db.cursor()
        
        sql="insert into Movie values ('"+casts+"','"+directors+"','"+rate+"','"+star+"','"+title+"','"+url+"')"
        #sql = sql.encode('gbk')
        print(sql)
        cursor.execute(sql)
        db.commit()
        db.close()
 

    


    
    #print(directors)


# In[ ]:




# In[41]:




# In[ ]:



