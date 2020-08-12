# -*- coding: utf-8 -*-

import requests
import json

url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/sentiment_classify?charset=UTF-8&access_token=24.4a0df9c2b85457ef210e710f4aef73db.2592000.1599822375.282335-21958456'
body = {"text":"我好开心"}
headers = {'content-type': "application/json"}

# token 24.4a0df9c2b85457ef210e710f4aef73db.2592000.1599822375.282335-21958456
# print type(body)
# print type(json.dumps(body))
# 这里有个细节，如果body需要json形式的话，需要做处理
# 可以是data = json.dumps(body)
response = requests.post(url, data=json.dumps(body), headers=headers)
# 也可以直接将data字段换成json字段，2.4.3版本之后支持
# response  = requests.post(url, json = body, headers = headers)

# 返回信息
print(response.text)
# 返回响应头
print(response.status_code)