import json
import os
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import get_json_object, udf
os.environ['SPARK_HOME'] =r'/Users/shuke/spark-2.11'
sys.path.append(r'/Users/shuke/spark-2.11')



area_str = 'file:///Users/shuke/Desktop/顶新/citys.json'
meta_str = '/Users/shuke/Desktop/顶新/92711130.json'


##解析城市信息得到省份 城市  以及对应的编码 编写UDF得到拼音
spark = SparkSession.builder.master('local[2]').getOrCreate()
df_area  = spark.read.json(r'/Users/shuke/Desktop/顶新/citys.json').createOrReplaceTempView('area_info')
spark.sql(" select get_json_object(cast(_corrupt_record as string),'$.code') from area_info limit 1 ").show()
#df_area.createOrReplaceTempView('area_info')
#spark.sql('select _corrupt_record.name from area_info').show()


#df_city.createOrReplaceTempView('area_info')
#spark.sql("select cityList.areaList.code,cityList.areaList.name from area_info").show