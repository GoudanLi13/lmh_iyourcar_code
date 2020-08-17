# -*- coding: utf-8 -*-

import requests
import json
import pyhs2
from pyhive import hive
import pandas as pd
import time


conn = hive.Connection(host='hiveserver2-prod.suv163.com',
                                  port=10000,
                                  username='liminghao',
                                  password='lmh19960613CS',
                                  database='iyourcar_dw',
auth='CUSTOM'
                                  )

list_1=[]
sql="select substr(text,0,800) from dws_prop_day_content where array_contains(car_series_ids,'3070') and kind=2"


url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/sentiment_classify?charset=UTF-8&access_token=24.4a0df9c2b85457ef210e710f4aef73db.2592000.1599822375.282335-21958456'
headers = {'content-type': "application/json"}
cursor=conn.cursor()
cursor.execute(sql)#执行查询
for result in cursor.fetchall():
    body = {"text": result}
    response = requests.post(url, data=json.dumps(body), headers=headers)
    dict=json.loads(response.text)
    s1=pd.Series(dict)
    try:
        df=pd.DataFrame(s1['items'])
        list_1.append(df['sentiment'][0])
    except KeyError as e:
        list_1.append(-1)
    time.sleep(1)
conn.close()
s2=pd.Series(list_1)
print(s2.value_counts())


#url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/sentiment_classify?charset=UTF-8&access_token=24.4a0df9c2b85457ef210e710f4aef73db.2592000.1599822375.282335-21958456'
#body = {"text":"我好开心"}
#headers = {'content-type': "application/json"}

# token 24.4a0df9c2b85457ef210e710f4aef73db.2592000.1599822375.282335-21958456
# print type(body)
# print type(json.dumps(body))
# 这里有个细节，如果body需要json形式的话，需要做处理
# 可以是data = json.dumps(body)
#response = requests.post(url, data=json.dumps(body), headers=headers)
# 也可以直接将data字段换成json字段，2.4.3版本之后支持
# response  = requests.post(url, json = body, headers = headers)

# 返回信息
#print(response.text)
# 返回响应头
#print(response.status_code)