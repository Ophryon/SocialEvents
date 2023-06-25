# -*- coding:utf-8 -*-
import pymongo
import numpy as np
import pandas as pd
import datetime
import json
# 链接到数据库
myclient = pymongo.MongoClient('')
# 创建一个数据库
mydbE = myclient["TwitterEvent2012"]
# 创建一个集合
mycolE = mydbE["tweets"]



# 读取数据后，转化成json格式，存储到mongodb
# 读取数据
def storage():
    df = np.load('TwitterEvent2012.npy', allow_pickle=True)
    data = pd.DataFrame(data=df, columns=["event_id", "tweet_id", "text", "created_at", "entity", "words"])
    print(data.shape)
    # 按行遍历数据
    for index, row in data.iterrows():
        info = {
            "_id": int(row["tweet_id"]),
            "event_id": int(row["event_id"]),
            "text": row["text"],
            "created_at": datetime.datetime.strftime(row["created_at"], '%Y-%m-%d')
        }
        try:
            x = mycolE.insert_one(info)
        except pymongo.errors.DuplicateKeyError:
            pass
    # 总计多少条数据
    print(mycolE.count_documents({}))

# 获得所有推文id，写入ids.txt
def getTweetId():
    Note=open('ids.txt',mode='a')
    rets = mycolE.find({})
    for ret in rets:
        # print(ret["_id"])
        Note.write(str(ret["_id"])+"\n")

# 注水 
# pip install twarc
# twarc configure
# twarc hydrate ids.txt > tweets.jsonl

# 推文详细信息存入Mongo
def writeDetail():
    Note=open('tweets.jsonl',mode='r',encoding='UTF-16')
    lines = Note.readlines()
    for line in lines:
        detail = json.loads(line)
        hashtag = []
        media = []
        user_mentions = []
        urls = []
        place = ""
        for index in range(len(detail['entities']["hashtags"])):
            hashtag.append(detail['entities']["hashtags"][index]["text"])

        if  "media" in detail['entities']:
            for index in range(len(detail['entities']["media"])) :
                media.append(detail['entities']["media"][index]["media_url"])

        for index in range(len(detail['entities']["user_mentions"])) :
            user_mentions.append(detail['entities']["user_mentions"][index]["id"])
        
        for index in range(len(detail['entities']["urls"])) :
            urls.append(detail['entities']["urls"][index]["url"])
     
        user_id = detail['user']["id"]

        if detail['place'] != None :
            place = detail['place']['name']

        condition = {'_id': detail["id"]}
        res = mycolE.update_one(condition,{"$set":{'hashtag': hashtag,'media': media,'user_mentions': user_mentions,'urls': urls,'user_id': user_id,'place': place}})
        # print(res)
        
def npyToCsv():
    # 可能存在问题
    # path处填入npy文件具体路径
    df = np.load('Data/TwitterEvent2012.npy', allow_pickle=True)
    data = pd.DataFrame(data=df, columns=["event_id", "tweet_id", "text", "created_at", "entity", "words"])
    data.to_csv('Data/TwitterEvent2012.csv')

npyToCsv()