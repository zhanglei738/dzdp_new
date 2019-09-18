import json
import os
import re
import sys
import jieba
import json
import pypinyin
from pyspark.sql import Row
from snownlp import SnowNLP
os.environ['SPARK_HOME'] =r'/Users/shuke/spark-2.3.4'
os.environ["PYSPARK_PYTHON"]=r'/usr/local/Cellar/python3/3.6.3/bin/python3.6'
sys.path.append(r'/Users/shuke/spark-2.3.4/python')
from pyspark.sql import SparkSession, Row
import time
from pyspark.sql.functions import format_number

#from pysparktest.Utils import parse_comment_info, get_band_info, get_row, row_to_dict,get_review_body_word, get_food_comment_grade, get_food_comment_row
import jieba.posseg
import jieba.analyse

#os.environ['SPARK_HOME'] =r'/data/app/spark-2.3.3-bin-hadoop2.6'
#sys.path.append(r'/data/app/spark-2.3.3-bin-hadoop2.6/python')
adj = {'酸': 0, '甜': 0, '苦': 0, '辣': 0, '麻': 0, '咸': 0, '甘': 0, '淡': 0, '香': 0}
adj2 = {'苦涩': 0, '平淡': 0, '鲜美': 0, '诱人': 0, '独特': 0, '醇厚': 0, '甜美': 0, '香辣': 0, '油腻': 0, '清香': 0, '甜蜜': 0, '甘甜': 0,
            '浓香': 0, '轻香': 0, '松脆': 0, '甘苦': 0,'咖喱' : 0}
print(time.time())

file = '/data/app/pysparkPro/config/7212_sogoucell_weijue.txt'
f = open(file, "r", encoding="gbk")
lines = f.readlines()
f.close()
tasteadj = {}
for line in lines:
     tasteadj[line.strip("\n").strip("的").strip("味")] = 0
for key in adj:
    tasteadj[key] = adj[key]
for key in adj2:
    tasteadj[key] = adj2[key]
for key in tasteadj:
    jieba.add_word(key)
def as_num(x):
  y = '{:.6f}'.format(x)  # 7f表示保留7位小数点的float型
  return y

with open('/data/app/pysparkPro/config/citys.json', 'r', encoding='utf-8') as w1:
    lines = w1.read()
    city_obj = json.loads(lines)
# 获取城市编码
def get_citycode(province_name, city_name,city_obj):
    city_code = ''
    for provinces in city_obj:
        # print(citys)
        if provinces['name'] != province_name: continue
        city_list = provinces['cityList']
        for city in city_list:
            if city['name'] == city_name:
                city_code = city['code']
                break
    return city_code

# 获取省份编码
def get_provincecode(province_name,city_obj):
    province_code = ''
    for provinces in city_obj:
        if provinces['name'] == province_name:
            province_code = provinces['code']
            break

    return province_code

# 获取地区编码
def get_districtcode(province_name, city_name, district_name,city_obj):
    district_code = ''
    for provinces in city_obj:
        if provinces['name'] != province_name: continue
        city_list = provinces['cityList']
        for city in city_list:
            if city['name'] != city_name: continue
            district_list = city['areaList']
            for districts in  district_list:
                if districts['name'] == district_name:
                    district_code = districts['code']
                    break

    return district_code
# 得到拼音首字母
def get_pinyin_initail(str_input):
    a = pypinyin.pinyin(str_input, style=pypinyin.FIRST_LETTER)
    b = []
    for i in range(len(a)):
        b.append(str(a[i][0]).upper())
    c = ''.join(b)
    return c
def get_band_info(line,city_obj):
    jshop = json.loads(line)
    brand_dic = {}
    brand_dic['shopid'] = jshop['shopId']
    if 'taste' in jshop:
        brand_dic['taste'] = jshop['taste'] if jshop['taste'] != '' and jshop['taste'] is not None  else 0
    else:
        brand_dic['taste'] = 0

    if 'environment' in jshop:
        brand_dic['environment'] = jshop['environment'] if jshop['environment'] != '' and jshop['environment'] is not None  else 0
    else:
        brand_dic['environment'] = 0

    if 'service' in jshop:
        brand_dic['service'] = jshop['service'] if jshop['service'] != '' and jshop['service'] is not None else 0
    else:
        brand_dic['service'] = 0

    if 'price' in jshop:
        price = jshop['price']
    else:
        price = 0

    if 'star' in jshop:
        brand_dic['star'] = jshop['star'] if jshop['star'] != '' and jshop['star'] is not None  else 0
    else:
        brand_dic['star'] = '0'

    if 'shopName' in jshop:
        brand_dic['shopName'] = jshop['shopName'] if jshop['shopName'] != '' and jshop['shopName'] is not None  else ''
    else:
        brand_dic['shopName'] = jshop['shopName']

    brand_id=1
    if '德克士' in brand_dic['shopName']:
        brand_id = 1
    elif '肯德基' in brand_dic['shopName']:
        brand_id = 2
    elif '麦当劳' in brand_dic['shopName']:
        brand_id = 3
    elif '康师傅' in brand_dic['shopName']:
        brand_id = 4
    elif '那不乐思' in brand_dic['shopName']:
        brand_id = 5
    elif '和府捞面' in brand_dic['shopName']:
        brand_id = 6
    elif '不列德' in brand_dic['shopName']:
        brand_id = 7
    elif '汉堡王' in brand_dic['shopName']:
        brand_id = 8
    elif '百年义利' in brand_dic['shopName']:
        brand_id = 9
    elif '稻香村' in brand_dic['shopName']:
        brand_id = 10
    elif '多乐之日' in brand_dic['shopName']:
        brand_id = 11
    elif '好利来' in brand_dic['shopName']:
        brand_id = 12

    if 'address' in jshop:
        brand_dic['address'] = jshop['address'] if jshop['address'] != '' and jshop['address'] is not None  else ''
    else:
        brand_dic['address'] = ''

    brand_dic['province_name'] = ''
    brand_dic['formatted_address'] = ''
    brand_dic['city_name'] = ''
    brand_dic['city_code'] = ''
    brand_dic['province_code'] = ''
    brand_dic['district_name'] = ''
    brand_dic['district_code'] = ''
    brand_dic['township_name'] = ''

    if 'AddressData' in jshop:
        address_data = jshop['AddressData']

        if 'glng' in address_data:
            brand_dic['lon'] = address_data['glng']
        else:
            brand_dic['lon'] = 0.0

        if 'glat' in address_data:
            brand_dic['lat'] = address_data['glat']
        else:
            brand_dic['lat'] = 0.0

        if 'regeocode' in address_data:
            regeocode_list = address_data['regeocode']

            if type(regeocode_list['formatted_address']) == list:
                brand_dic['formatted_address'] = ''
            else:
                brand_dic['formatted_address'] = regeocode_list['formatted_address']

            if 'addressComponent' in regeocode_list:
                addressComponent = regeocode_list['addressComponent']

                if type(addressComponent['province']) == list:
                    brand_dic['province_name'] = ''
                else:
                    brand_dic['province_name'] = addressComponent['province']

                if type(addressComponent['city']) == list:
                    brand_dic['city_name'] = brand_dic['province_name']
                else:
                    brand_dic['city_name'] = addressComponent['city']

                if type(addressComponent['district']) == list:
                    brand_dic['district_name'] = ''
                else:
                    brand_dic['district_name'] = addressComponent['district']

                if type(addressComponent['township']) == list:
                    brand_dic['township_name'] = ''
                else:
                    brand_dic['township_name'] = addressComponent['township']

                brand_dic['city_code'] = get_citycode(brand_dic['province_name'], brand_dic['city_name'],city_obj)
                brand_dic['province_code'] = get_provincecode(brand_dic['province_name'],city_obj)
                brand_dic['district_code'] = get_districtcode(brand_dic['province_name'], brand_dic['city_name'],brand_dic['district_name'],city_obj)

                brand_dic['province_initial'] = get_pinyin_initail(brand_dic['province_name'])
                brand_dic['city_initial'] = get_pinyin_initail(brand_dic['city_name'])
                brand_dic['district_initial'] = get_pinyin_initail(brand_dic['district_name'])
                #print(brand_dic['price'])
    #return Row(price=price)
    return Row(brand=brand_dic['shopName'],brand_id=brand_id,shop_id=brand_dic['shopid'],shop_name=brand_dic['shopName'] + brand_dic['address'],taste=brand_dic['taste']
               ,env=brand_dic['environment'],service=brand_dic['service'], price=price, star=brand_dic['star'], province=brand_dic['province_name'],
               province_code=brand_dic['province_code'],province_initial=brand_dic.get('province_initial',''),city=brand_dic['city_name'],city_code=brand_dic['city_code']
               ,city_initial=brand_dic.get('city_initial',''),district=brand_dic['district_name'],district_code=brand_dic['district_code'],district_initial=brand_dic.get('district_initial',''),
               township=brand_dic['township_name'],formatted_address=brand_dic['formatted_address'],lon=brand_dic['lon'],lat=brand_dic['lat'],batch='201905')

def parse_comment_info(line):
    jshop = json.loads(line)
    comment_data = jshop['CommentData']
    comment_list = comment_data['comment']
    shop_id = jshop['shopId']
    shop_name = jshop['shopName']
    brand_id=1
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
    elif '不列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    elif '百年义利' in shop_name:
        brand_id = 9
    elif '稻香村' in shop_name:
        brand_id = 10
    elif '多乐之日' in shop_name:
        brand_id = 11
    elif '好利来' in shop_name:
        brand_id = 12
    index = 1
    commeent_list = []
    for comment in comment_list :
        review_body = str(comment['reviewBody']).replace('\r','').replace("|",'')
        add_time = str(comment['addTime']).replace('T', ' ').replace('Z','')
        star = str(comment['star'])
        nick_name = str(comment['userNickName']).replace('\"', '').replace('＂','')
        #if len(lines) != 5: continue
        if review_body == '': continue
        s = SnowNLP(review_body)
        comment_score = float(as_num(s.sentiments))
        catetory = 0
        if(comment_score >=0.8):
            catetory = 1
        elif(comment_score <0.5):
            catetory = 3
        else:
            catetory = 2
        index += 1
        commeent_list.append(('%s| %s | %s | %s |%s | %s | %s | %s |%s' % (index, review_body,comment_score, add_time, star, nick_name,brand_id,shop_id,catetory)))
    return commeent_list

def get_row(str):
    strspi = str.split("|")
    try:
        comment_score = float(strspi[2])
        add_time  = strspi[3]
        star = int(strspi[4])
        nick_name = strspi[5]
        brand_id=strspi[6]
        shop_id = strspi[7]
    except:
        print(str)
        comment_score = float(strspi[3])
        add_time  = strspi[4]
        star = int(strspi[5])
        nick_name = strspi[6]
        brand_id=strspi[7]
        shop_id = strspi[8]
    return Row(batch='201905',comment_score=comment_score,add_time=add_time,star=star,nick_name=nick_name,brand_id=brand_id,shop_id=shop_id)


def get_review_body_word(str):
    strspi = str.split("|")
    brand_id = strspi[0]
    shop_id=strspi[1]
    catetory=strspi[2]
    key = strspi[3]
    value = strspi[4]
    return Row(brand_id=brand_id,shop_id=shop_id,comment_category=catetory,word=key,value=value,batch='201905')

def get_food_comment_grade(line,menu_list):
    jshop = json.loads(line)
    comment_data = jshop['CommentData']
    comment_list = comment_data['comment']
    shop_id = jshop['shopId']
    menu_dic = {}
    dish_list = []
    for key in menu_list:
        shop_dish = key.split('|')
        dish_shop_id = shop_dish[0]
        dish_dish_name =  shop_dish[1]
        if(shop_id == dish_shop_id):
            dish_list.append(dish_dish_name)
            menu_dic[dish_dish_name] = {}
            jieba.add_word(dish_dish_name)
        else:
            pass
    shop_name = jshop['shopName']
    brand_id=1
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
    elif '不列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    elif '百年义利' in shop_name:
        brand_id = 9
    elif '稻香村' in shop_name:
        brand_id = 10
    elif '多乐之日' in shop_name:
        brand_id = 11
    elif '好利来' in shop_name:
        brand_id = 12
    food_comment_list = []
    for comment in comment_list :
        review_body = str(comment['reviewBody']).replace('\r','')
        if review_body == '': continue
        s = SnowNLP(review_body)
        for food_name in dish_list:
            if food_name in review_body:
                food_comment_list.append(('%s|%s|%s |%s |%s ' % (brand_id,shop_id,food_name,str(s.sentiments),'201905')))
    return food_comment_list

def get_food_comment_row(str):
    strspi = str.split("|")
    brand_id = strspi[0]
    shop_id=strspi[1]
    food_name=strspi[2]
    grade = strspi[3]
    batch = strspi[4]
    return Row(brand_id=brand_id,shop_id=shop_id,food_name=food_name,grade=grade,batch=batch)
def row_to_dict(row):
    dict_row = row.asDict()
    name_row = {}
    if 'nick_name' in dict_row:
        v = dict_row['nick_name']
        name_row['nick_name']=v
    return name_row
#获取评论词云
def get_count_word(line):
    jshop = json.loads(line)
    comment_data = jshop['CommentData']
    comment_list = comment_data['comment']
    shop_id = jshop['shopId']
    shop_name = jshop['shopName']
    brand_id=1
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
    elif '不列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    elif '百年义利' in shop_name:
        brand_id = 9
    elif '稻香村' in shop_name:
        brand_id = 10
    elif '多乐之日' in shop_name:
        brand_id = 11
    elif '好利来' in shop_name:
        brand_id = 12
    commeent_list = []
    keywords = {}
    if(len(comment_list)>0):
        for comment in comment_list :
            review_body = str(comment['reviewBody']).replace('\r','')
            if review_body == '': continue
            allowpos=('a','an' 'y','e')
            minstat = 3
            seg_list = jieba.analyse.extract_tags(review_body, topK=20, allowPOS=allowpos)
            # print(seg_list)
            for z in seg_list:
                if z in keywords.keys():
                    keywords[z] = keywords[z] + 1
                else:
                    keywords[z] = 1
        for key in keywords.keys():
            if(keywords[key] >= 3):
                commeent_list.append(('%s |%s |%s | %s |%s ' % (brand_id,shop_id,key,keywords[key],'201905')))
        return commeent_list
    else:
        pass
def get_count_word_row(line):
    strspi = line.split("|")
    return Row(brand_id=strspi[0],shop_id=strspi[1],word=strspi[2],count=strspi[3],batch=strspi[4])

def get_taste_word_row(line):
    strspi = line.split("|")
    return Row(brand_id=strspi[0],shop_id=strspi[1],category=strspi[2],value=strspi[3],batch=strspi[4])
#获取口味的方法
def get_taste_word(line):
    jshop = json.loads(line)
    comment_data = jshop['CommentData']
    comment_list = comment_data['comment']
    shop_name = jshop['shopName']
    shop_id = jshop['shopId']
    brand_id=1
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
    elif '不列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    elif '百年义利' in shop_name:
        brand_id = 9
    elif '稻香村' in shop_name:
        brand_id = 10
    elif '多乐之日' in shop_name:
        brand_id = 11
    elif '好利来' in shop_name:
        brand_id = 12
    commeent_list = []
    for comment in comment_list :
        review_body = str(comment['reviewBody']).replace('\r','')
        if review_body == '': continue
        seg_list = jieba.cut(review_body, cut_all=True)
        commeent_list = []
        for z in seg_list:
            if z in tasteadj.keys():
                tasteadj[z] = tasteadj[z] + 1
            else:
                pass
        for key in tasteadj.keys():
            if tasteadj[key] > 0 and key in adj.keys():
                commeent_list.append(('%s |%s |%s | %s |%s ' % (brand_id,shop_id,key,tasteadj[key],'201905')))
    return commeent_list
with open('/data/app/pysparkPro/config/citys.json', 'r', encoding='utf-8') as w1:
    lines = w1.read()
    city_obj = json.loads(lines)
def etl_dish_json(line):
    jshop = json.loads(line)
    dish_list = jshop['dish']
    shop_id = jshop['shopId']
    shop_name = jshop['shopName']
    brand_id=1
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
    elif '不列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    elif '百年义利' in shop_name:
        brand_id = 9
    elif '稻香村' in shop_name:
        brand_id = 10
    elif '多乐之日' in shop_name:
        brand_id = 11
    elif '好利来' in shop_name:
        brand_id = 12
    output_dict = []
    for dish in dish_list:
        dishName = dish['dishName']
        addTime = str(dish['addTime']).replace('T', ' ').replace('Z','')
        dishPrice = dish['price'] if dish['price'] != '' else 0
        output_dict.append(('%s |%s |%s |%s | %s |%s ' % (dishName,dishPrice,addTime,brand_id,shop_id,'201905')))
    return output_dict

def get_each_dish(line):
    strspi = line.split("|")
    return (strspi[4].strip()+'|'+strspi[0].strip(),strspi[1].strip()+"|"+strspi[2].strip())
    #return (strspi[0].strip(),strspi[1].strip()+"|"+strspi[2].strip())

def get_dish_comment(line,menu_list):
    jshop = json.loads(line)
    comment_data = jshop['CommentData']
    comment_list = comment_data['comment']
    shop_id = jshop['shopId']
    menu_dic = {}
    dish_list = []
    for key in menu_list:
        shop_dish = key.split('|')
        dish_shop_id = shop_dish[0]
        dish_dish_name =  shop_dish[1]
        if(shop_id == dish_shop_id):
            menu_dic[dish_dish_name] = {}
            dish_list.append(dish_dish_name)
            jieba.add_word(dish_dish_name)
    shop_name = jshop['shopName']
    brand_id=1
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
    elif '不列德' in shop_name:
        brand_id = 7
    elif '汉堡王' in shop_name:
        brand_id = 8
    elif '百年义利' in shop_name:
        brand_id = 9
    elif '稻香村' in shop_name:
        brand_id = 10
    elif '多乐之日' in shop_name:
        brand_id = 11
    elif '好利来' in shop_name:
        brand_id = 12
    food_word_list = []
    #dic_i = {}
    for comment in comment_list :
        review_body = str(comment['reviewBody']).replace('\r','')
        seg_list = jieba.cut(review_body, cut_all=True)
        dic_i = {}
        for z in seg_list:
            if z in dic_i.keys() : dic_i[z] = dic_i[z] + 1
            elif z in tasteadj.keys() : dic_i[z] = 1
            else : pass
        # print(dic_i)
        for c in dish_list:
            if c in review_body:
                for x in dic_i.keys():
                    if x in menu_dic[c].keys():
                        menu_dic[c][x] = menu_dic[c][x] + dic_i[x]
                    else:
                        menu_dic[c][x] = dic_i[x]
    for foodname in menu_dic:
        for wc in menu_dic[foodname]:
            food_word_list.append(('%s|%s|%s |%s |%s|%s ' % (brand_id,shop_id,foodname,wc,menu_dic[foodname][wc],'201905')))
    return food_word_list

def get_food_word_row(line):
    strspi = line.split("|")
    return Row(brand_id=strspi[0],shop_id=strspi[1],food_name=strspi[2],word=strspi[3],count=strspi[4],batch=strspi[5])

def get_food_arrival_row(line):
    strspi = line.split("|")
    #dishName,dishPrice,addTime,brand_id,shop_id,'201905'
    return Row(dish_name=strspi[0],dish_price=strspi[1],dish_time=strspi[2],brand_id=strspi[3],shop_id=strspi[4],batch=strspi[5])

def get_category_comment_word(line):
    allowpos=('a','an' 'y','e')
    strspi = line.split("|")
    review_body = strspi[1]
    shop_id=strspi[7]
    brand_id=strspi[6]
    catetory = strspi[8]
    keywords = {}
    seg_list = jieba.analyse.extract_tags(review_body, topK=20, allowPOS=allowpos)
    for z in seg_list:
        if z in keywords.keys():
            keywords[z] = keywords[z] + 1
        else:
            keywords[z] = 1
    word_list = []
    for key in keywords.keys():
        word_key = ('%s | %s | %s | %s |%s ' % (brand_id,shop_id,catetory,key,keywords[key]))
        word_list.append(word_key)
    return word_list
#菜品评论相关
menu_list=["薯条","手枪腿"]
menu_dic = {}
for key in menu_list:
    menu_dic[key] = {}
#spark_conf =
spark = SparkSession.builder.config("spark.driver.memory","2g").getOrCreate()
sc = spark.sparkContext

json_file = sc.textFile(r"D:\工作\新建文件夹 (2)\all8。txt").repartition(200)
json_file.cache()
#json_file.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
#1.获取店铺信息
infor_map = json_file.map(lambda line:get_band_info(line,city_obj))
df_info = spark.createDataFrame(infor_map)
prop = {}
prop['user'] = 'root'
prop['password'] = 'admin@123456'
prop['driver'] = "com.mysql.jdbc.Driver"
df_info.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop','overwrite', prop)

#2.得到词频
review_flatmap = json_file.flatMap(get_count_word)
review_map = review_flatmap.map(get_count_word_row)
df_review_word = spark.createDataFrame(review_map)
df_review_word.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop_word_cloud','overwrite', prop)

#3.得到标准口味
taste_word_flatmap = json_file.flatMap(get_taste_word)
taste_word_map = taste_word_flatmap.map(get_taste_word_row)
df_taste_word = spark.createDataFrame(taste_word_map)
df_taste_word.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop_taste_radar','overwrite', prop)

#4、得到菜品评论
dish_flatmap = json_file.flatMap(etl_dish_json)
dish_flatmap.cache()
dish_map = dish_flatmap.map(get_each_dish)
dup_dish = dish_map.groupByKey().map(lambda x_y: list(x_y)[0]).collect()
dish_comment_flatmap = json_file.flatMap(lambda x:get_dish_comment(x,dup_dish))
dish_comment_map = dish_comment_flatmap.map(get_food_word_row)
df_dish_comment = spark.createDataFrame(dish_comment_map)
df_dish_comment.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop_food_word_cloud','overwrite', prop)

#5、得到新品上架时间
# food_arrival_map = dish_flatmap.map(get_food_arrival_row)
# df_food_arrival = spark.createDataFrame(food_arrival_map)
# df_food_arrival.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_test?characterEncoding=utf8&useSSL=false",'brand_shop_food_new_arrival','overwrite', prop)



# 6、 得到店铺 - 评论情感  brand_shop_food_comment
food_comment_flatmap = json_file.flatMap(parse_comment_info)
food_comment_map = food_comment_flatmap.map(lambda x:get_row(x))
df_food_comment = spark.createDataFrame(food_comment_map)
df_food_comment.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop_food_comment','overwrite', prop)
#
#7、得到评论情感得分   brand_shop_food_category_comment_word_cloud
category_comment_flatMap= food_comment_flatmap.flatMap(get_category_comment_word)
category_comment_map = category_comment_flatMap.map(get_review_body_word)
df_category_comment = spark.createDataFrame(category_comment_map)
count_category_comment = df_category_comment.groupBy('brand_id','comment_category','shop_id','word','batch').count()
each_shop_category_comment = count_category_comment.filter(count_category_comment['count']>=2)
df_value = each_shop_category_comment.groupBy('shop_id').sum('count')
df_category_comment_mid_result = each_shop_category_comment.join(df_value,each_shop_category_comment['shop_id'] == df_value['shop_id']).drop(df_value.shop_id)
df_category_comment_result = df_category_comment_mid_result.withColumn('count_ratio',format_number(df_category_comment_mid_result['count']/(df_category_comment_mid_result['sum(count)']),4))
df_category_comment_result.drop('sum(count)').write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop_food_category_comment_word_cloud','overwrite', prop)
#

# 8、得到评论中菜品情感得分
food_comment_grade_flatmap = json_file.flatMap(lambda line:get_food_comment_grade(line,dup_dish))
food_comment_grade_map = food_comment_grade_flatmap.map(get_food_comment_row)
df_comment_grade = spark.createDataFrame(food_comment_grade_map)
df_comment_grade.write.jdbc("jdbc:mysql://172.16.111.190:3306/words_cloud_800?characterEncoding=utf8&useSSL=false",'brand_shop_food_comment_grade','overwrite', prop)
print(time.time())






