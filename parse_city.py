import json
import os
import re
import sys

with open('/Users/shuke/Desktop/顶新/citys.json', 'r', encoding='utf-8') as w1:
    lines = w1.read()
    city_obj = json.loads(lines)
# def city_code_udf(city_name):
#     city_code = ''
#     for provinces in city_obj:
#         # print(citys)
#         city_list = provinces['cityList']
#         for city in city_list:
#             if city['name'] == city_name:
#                 city_code = city['code']
#                 break
#     return city_code
    for x in city_obj :
        print(x)