"""
A simple example demonstrating Spark dataframe features and redis.
"""
#from __future__ import print_function

# $example on:init_session$
from pyspark.sql import SparkSession
# $example off:init_session$

import redis
from datetime import datetime, timezone
# from _datetime import timezone

# varilabes to sto re redis info
redis_host = "localhost"
redis_port = 6379
redis_password = ""

# initialize sample dictionary variable
ds = dict(brand="brand",colors="colors",dateAdded=12234556678.0)

# redis client variable
# redisCli = redis.client
redisCli = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
from flask import Flask, jsonify
from flask_restful import Resource, Api
# from json import dumps
# from flask import 

app = Flask(__name__)
api = Api(app)

def getDateStr(dateFloat):
  return datetime.fromtimestamp(dateFloat)

def getTimeStamp(dateStr):
  ts1 = 0
  year = month = day = hour = mint = sec = 0
  if dateStr is None:
    return ts1
  else:
    if len(dateStr) > 3:
      year = int(dateStr[0:4])
    if len(dateStr) > 6:
      month=int(dateStr[5:7])
    if len(dateStr) > 9:
      day=int(dateStr[8:10])
    if len(dateStr) > 12:
      hour=int(dateStr[11:13])
    if len(dateStr) > 15:
      mint=int(dateStr[14:16])
    if len(dateStr) > 18:
      sec=int(dateStr[17:19])
#     print(year, month, day, hour, mint, sec)
    ts1 = datetime(year, month, day, hour, mint, sec, 0, tzinfo=timezone.utc).timestamp()
    return ts1

def loadDataIntoRedis(spark):
    # spark is an existing SparkSession

    #read csv and load into dataframe
    df = spark.read.csv("sample.csv", header=True)

    rows = list(df.select(df['id'],df['brand'],df['colors'],df['dateAdded']).toLocalIterator())
    df = None

#     for r in rows:
#       print(r)
#     return

#     connect to redis and save dataframe
    try:
      redisCli.flushall()
      for r in rows:
        if r.brand is not None and r.id is not None and r.colors is not None and r.dateAdded is not None:
          hashName = "customer:"+r.id
          brand = r.brand.lower()
          color = r.colors.lower()
          if False == redisCli.exists(hashName):
            ds.__setitem__("brand", brand)
            ds.__setitem__("colors", color)
            ts = getTimeStamp(r.dateAdded)
            ds.__setitem__("dateAdded", ts)
            redisCli.hmset(hashName, ds)
            redisCli.lpush(ts, r.id)
            cs = color.split(',')
            if len(cs) > 1:
              for c in cs:
                redisCli.lpush(c, r.id)
            else:
              redisCli.lpush(color, r.id)
            redisCli.zincrby("count", brand, 1)
    except Exception as e:
      print(e)
      return
    print('APPLICATION IS READY. RUN CURL COMMAND TO TEST')
    return

class GetRecentItem(Resource):
    def get(self,dateAdded):
      result = {} #dict(id ="", brand="",colors="",dateAdded="")
      try:
        memberList = redisCli.lrange(getTimeStamp(dateAdded), 0, 0)
        if len(memberList) > 0:
          print('RESULT IS ',memberList)
          result = redisCli.hgetall("customer:"+memberList[0])
          if len(result) > 0:
            print('RESULT IS ',result)
            result.__setitem__("id", memberList[0])
#             result.__setitem__("brand", res2.brand)
#             result.__setitem__("colors", res2.colors)
            result.__setitem__("dateAdded", dateAdded)
      except Exception as e:
        print(e)
        result = {'result':e}
      return result

class GetBrandsCount(Resource):
    def get(self):
      result = {}
      try:
        res = redisCli.zrange(name="count", start=0, end=-1, desc=True, withscores=True)
        if len(res) > 0:
          result = res
      except Exception as e:
          print(e)
          result = {'count':e}
      return jsonify(result)

class GetItemsbyColor(Resource):
    def get(self, color):
      result = []
      memberInfo = {}

      try:
        memberList = redisCli.lrange(color.lower(), 0, 9)
        if len(memberList) > 0:
          print('RESULT IS ',memberList)
          for m in memberList:
            memberInfo = redisCli.hgetall("customer:"+m)
            if len(memberInfo) > 0:
              memberInfo.__setitem__("id", m)
              memberInfo.__setitem__("dateAdded", getDateStr(float(memberInfo.get("dateAdded"))))
              result.append(memberInfo)
      except Exception as e:
        print(e)
        result = {'result':e}
      return jsonify(result)

api.add_resource(GetRecentItem, '/getRecentItem/<dateAdded>') # Route_1
api.add_resource(GetBrandsCount, '/getBrandsCount') # Route_2
api.add_resource(GetItemsbyColor, '/getItemsbyColor/<color>') # Route_3

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("spark-redis example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$

    loadDataIntoRedis(spark)
    spark.stop()

#     listen and server REST APIs
    app.run(port='5002')
