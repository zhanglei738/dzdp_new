import json
import os
import re
import sys
import pypinyin

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
os.environ['SPARK_HOME'] =r'/Users/shuke/spark-2.3.4'
os.environ["PYSPARK_PYTHON"]=r'/usr/local/Cellar/python3/3.6.3/bin/python3.6'
sys.path.append(r'/Users/shuke/spark-2.3.4/python')
with open('/Users/shuke/Desktop/顶新/citys.json', 'r', encoding='utf-8') as w1:
    lines = w1.read()
    city_obj = json.loads(lines)
def brand_id_udf(shop_name):
    brand_id=0
    if '德克士' in shop_name:
        brand_id = 1
    elif '肯德基' in shop_name:
        brand_id = 2
    elif '麦当劳' in shop_name:
        brand_id = 3
    elif '康师傅' in shop_name:
        brand_id = 4
    elif '那不乐思' in shop_name:
        brand_id = 5
    elif '和府捞面' in shop_name:
        brand_id = 6
    elif '布列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    else:
        brand_id = 0
    return brand_id
#获取City code
def city_code_udf(city_name):
    city_code = ''
    for provinces in city_obj:
        # print(citys)
        city_list = provinces['cityList']
        for city in city_list:
            if city['name'] == city_name:
                city_code = city['code']
                break
    return city_code
#获取 province code
def province_code_udf(province_name):
    province_code = ''
    for x in city_obj :
        if x['name'] == province_name :
            province_code = x['code']
            break
    return province_code

#拼音
def get_pinyin_initail(str_input):
    a = pypinyin.pinyin(str_input, style=pypinyin.FIRST_LETTER)
    b = []
    for i in range(len(a)):
        b.append(str(a[i][0]).upper())
    c = ''.join(b)
    return c


spark = SparkSession.builder.master('local[2]').getOrCreate()
##注册获取品牌的UDF
spark.udf.register("brand_id_udf", lambda s: brand_id_udf(s), "int")
##注册获取城市编码
spark.udf.register("city_code_udf",lambda s:city_code_udf(s),"string")
##获取省份编码
spark.udf.register("province_code_udf",lambda s:province_code_udf(s),"string")
##注册拼音udf
spark.udf.register("get_pinyin_initail",lambda s:get_pinyin_initail(s),"string")

df = spark.read.json(r'/Users/shuke/Desktop/顶新/92711130.json')
tmp = df.createOrReplaceTempView('brand_shop_info')
querys = """
       select shopid,taste,environment,
       service,price,star,shopName,
       brand_id_udf(shopName) as  band_id,
       address,AddressData.glng,
       AddressData.glat,
       AddressData.regeocode.formatted_address,
       AddressData.regeocode.addressComponent.province as province_name,
       province_code_udf(AddressData.regeocode.addressComponent.province) as province_code,
       get_pinyin_initail(AddressData.regeocode.addressComponent.province) as province_initail,
       AddressData.regeocode.addressComponent.city as city_name,
       city_code_udf(AddressData.regeocode.addressComponent.city) as city_code,
       get_pinyin_initail(AddressData.regeocode.addressComponent.city) as city_initail,
       AddressData.regeocode.addressComponent.district as district_name,
       AddressData.regeocode.addressComponent.township as township_name 
       from brand_shop_info"""

querys1="""
         select shopName,
         AddressData.regeocode.addressComponent.province as province_name,
         province_code_udf(AddressData.regeocode.addressComponent.province) as province_code,
         get_pinyin_initail(AddressData.regeocode.addressComponent.province) as province_initail,
         AddressData.regeocode.addressComponent.city as city_name,
         city_code_udf(AddressData.regeocode.addressComponent.city) as city_code,
         get_pinyin_initail(AddressData.regeocode.addressComponent.city) as city_initail
         from  brand_shop_info   limit 10
        """
spark.sql(querys).write.format("hive").mode("overwrite").saveAsTable("xxzy.ods_shop_info_m")
#获取城市编码


