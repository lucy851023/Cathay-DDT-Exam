# import packages
import findspark
findspark.init() 
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import struct
from pyspark.sql import Window
from pyspark.sql.functions import sort_array
import os
from pyspark import SparkContext
import json

# Bulid Spark Session
spark=SparkSession.builder.appName('Practice').getOrCreate()

# Set the file names and path of data we want to read
cities = ['A','B','E','F','H']
city_dic = {'A':'台北市','B':'台中市','E':'高雄市','F':'新北市','H':'桃園市'}
tails = '_lvr_land_A.csv'
data_dir = './data'

# Read dataset
final_df = None
city_count = 0
for city in cities:
    fname = city + tails
    fpath = os.path.join(data_dir,fname)
    df = spark.read.option('header','true').csv(fpath)
    dfWithIndex = df.withColumn("id",monotonically_increasing_id()) # build id for each row
    dfWithIndex = dfWithIndex.where("id != 0") # remove english title (id==0)
    dfWithCity = dfWithIndex.withColumn("city",lit(city_dic[city])) # Add column : city
    if city_count == 0:
        final_df = dfWithCity
    else:
        final_df = final_df.unionAll(dfWithCity) # Union All files in dataset
    city_count+=1

final_df.printSchema()

## Define the function

# The transformation of number of floors : Transform chinese words to numeric values
def text2num(text):
    text2num_dic = {'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10}
    if text is None:
        return -1
    if len(text) > 0:
        text = text.replace("層","") # 移除"層"
    if len(text) == 1:
        return text2num_dic[text]
    elif len(text) == 2:
        if text[0] == '十':
            return 10+text2num_dic[text[-1]]
        elif text[-1] == '十':
            return text2num_dic[text[0]]*10
        else:
            return -1
    elif len(text)==3:
        if text[1] == '十':
            return text2num_dic[text[0]]*10+text2num_dic[text[-1]]
        else:
            return -1
    else:
        return -1 
# The transformation of date
def dateformat(string):
    year = int(string[:3]) + 1911
    new_string = str(year) + '-' + string[3:5] + '-' + string[5:]
    return new_string

# Set udf functions for use
udftext2num = F.udf(text2num, IntegerType())
udfdateformat = F.udf(dateformat, StringType())

# Add new column "floor"
floor_df = final_df.withColumn("floor", udftext2num("總樓層數"))

# Select data based on the condition
selected_df = floor_df.filter((col("主要用途") =='住家用') & (col("建物型態").like('%住宅大樓%')) & (col("floor") >= 13))
selected_df.select("主要用途","建物型態","floor").show(10)

# Select necessary columns for json file output
output_df = selected_df.select("city","交易年月日","鄉鎮市區","建物型態")

# Date format transformation
output_df = output_df.withColumn("交易年月日",udfdateformat("交易年月日"))
output_df.printSchema()

# Rname columns
output_df = output_df.withColumnRenamed("交易年月日","date").withColumnRenamed("鄉鎮市區","district").withColumnRenamed("建物型態","building_state")

# Define the structure of dataframe for json file output
event_df = output_df.groupby('city','date').agg(F.collect_list(F.struct("building_state","district")).alias('event'))
print(event_df.count())
struct_df2 = event_df.groupby('city').agg(sort_array(F.collect_list(F.struct("date","event")),asc=False).alias('time_slots'))

# Turn dataframe to Json
json_data = struct_df2.toJSON()
# Random split the json into two json
json1,json2 = json_data.randomSplit([1,1],0)

# Turn Pipelined RDD type to list
json1_list = json1.collect()
json2_list = json2.collect()

# Ouput Json
with open("result-part1_v2.json", 'w') as f:
    json.dump(json1_list,f)
with open("result-part2_v2.json", 'w') as f:
    json.dump(json2_list,f)

# Ouput Json v2
with open("result-part1.json", 'w') as f:
    for i in range(len(json1_list)):
        f.write(json1_list[i])
        if i < len(json1_list)-1:
            f.write("\n")

with open("result-part2.json", 'w') as f:
    for i in range(len(json2_list)):
        f.write(json2_list[i])
        if i < len(json2_list)-1:
            f.write("\n")