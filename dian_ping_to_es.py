import json
import os
import re
import sys
import jieba
from pyspark.sql import SparkSession, Row
import jieba.posseg
import jieba.analyse
# def get_each_str(line):
#     str_line = line.split("|")
#     add_time = str_line[0]
#     review_body = str_line[1]
#     nick_name = str_line[2]
#     star = str_line[3]
#     shopName = str_line[4]
#     shopid = str_line[5]
#     shopname_address = str_line[6]
#     province_name = str_line[7]
#     province_code = str_line[8]
#     city_name = str_line[9]
#     city_code = str_line[10]
#     district_name = str_line[11]
#     district_code = str_line[12]
#     township_name = str_line[13]
#     formatted_address = str_line[14]
#     return Row(add_time=add_time,review_body=review_body,nick_name=nick_name,star=star,shopName=shopName,shopid=shopid,shopname_address=shopname_address,province_name=province_name
#                ,province_code=province_code,city_name=city_name,city_code=city_code,district_name=district_name,district_code=district_code,township_name=township_name,formatted_address=formatted_address)
from snownlp import SnowNLP

from pysparktest.Utils import as_num


def get_reviw_tuple(line):
    str_line = line.split("|")
    add_time = str_line[0]
    review_body = str_line[1]
    shopName = str_line[3]
    shopid = str_line[4]
    shopname_address = str_line[5]
    province_name = str_line[6]
    province_code = str_line[7]
    city_name = str_line[8]
    city_code = str_line[9]
    district_name = str_line[10]
    district_code = str_line[11]
    township_name = str_line[12]
    formatted_address = str_line[13]
    catetory = '0'
    if(review_body ==''):
        print("not comment")
        catetory = '2'
    else:
        s = SnowNLP(review_body)
        comment_score = float(as_num(s.sentiments))
        if(comment_score >=0.8):
            catetory = '1'
        elif(comment_score <0.5):
            catetory = '3'
        else:
            catetory = '2'
    return (add_time+'|'+shopName+'|'+shopid+'|'+shopname_address+'|'+province_name+'|'+province_code+'|'+city_name+'|'+city_code+'|'+district_name+'|'+district_code+'|'+township_name+'|'+formatted_address+'|'+catetory,review_body)

def get_shop_word_count(line):
    basic = line[0]
    str_line = basic.split("|")
    add_time = str_line[0]
    shopName = str_line[1]
    shopid = str_line[2]
    shopname_address = str_line[3]
    province_name = str_line[4]
    province_code = str_line[5]
    city_name = str_line[6]
    city_code = str_line[7]
    district_name = str_line[8]
    district_code = str_line[9]
    township_name = str_line[10]
    formatted_address = str_line[11]
    catetory = str_line[12]
    review_list = line[1]
    keywords = {}
    for review in review_list:
        allowpos=('a','an' 'y','e')
        seg_list = jieba.analyse.extract_tags(review, topK=20, allowPOS=allowpos)
        # print(seg_list)
        for z in seg_list:
            if z in keywords.keys():
                keywords[z] = keywords[z] + 1
            else:
                keywords[z] = 1
    commeent_list = []
    for key in keywords.keys():
        commeent_list.append(('%s |%s |%s | %s |%s |%s |%s |%s | %s |%s |%s |%s |%s | %s |%s  ' %
                              (add_time,shopName,shopid,shopname_address,province_name,province_code,city_name,city_code,district_name,district_code,township_name,formatted_address,catetory,key,keywords[key])))
    return commeent_list

def get_shop_word_count_todf(line):
    str_line = line.split("|")
    add_time = str_line[0]
    shopName = str_line[1]
    shopid = str_line[2]
    shopname_address = str_line[3]
    province_name = str_line[4]
    province_code = str_line[5]
    city_name = str_line[6]
    city_code = str_line[7]
    district_name = str_line[8]
    district_code = str_line[9]
    township_name = str_line[10]
    formatted_address = str_line[11]
    catetory = str_line[12]
    key = str_line[13]
    count = str_line[14]
    return Row(add_time=add_time,word=key,count=count,shopName=shopName,shopid=shopid,shopname_address=shopname_address,province_name=province_name,catetory=catetory
               ,province_code=province_code,city_name=city_name,city_code=city_code,district_name=district_name,district_code=district_code,township_name=township_name,formatted_address=formatted_address)


os.environ['SPARK_HOME'] =r'D:\data\spark-2.3.3-bin-hadoop2.6'
sys.path.append(r'D:\data\spark-2.3.3-bin-hadoop2.6\python')
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
review_file = sc.textFile(r'D:\data\bakdianpdata\allfile\reviewBodyfile.txt')
review_group = review_file.map(get_reviw_tuple).groupByKey().mapValues(list)
review_word_count = review_group.flatMap(get_shop_word_count)

review_word_count_map = review_word_count.map(get_shop_word_count_todf)
spark.createDataFrame(review_word_count_map).write.csv(r'D:\data\bakdianpdata\allfile\allresult')
#df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))


