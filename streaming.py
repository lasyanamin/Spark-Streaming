from pprint import pprint
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from pymongo import MongoClient
import pymongo
import os
CONNECTION_STRING = "mongodb+srv://lasya:1996@cluster0.ectp0.mongodb.net/myFirstDatabase?retryWrites=true&w=majority/lasya_twitter.TwitterData"

def get_database():
    # connection string to mongo db
    CONNECTION_STRING = "mongodb+srv://lasya:1996@cluster0.ectp0.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    # create connection
    client = MongoClient(CONNECTION_STRING)

    return client['lasya_twitter']


import findspark
findspark.init('C:\datamaking\spark\spark-3.2.1-bin-hadoop3.2')

#function to calculate stateful tweets counts

def tweets_count(new_values, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(new_values,runningCount)

# running spark in local mode
conf = SparkConf().setMaster("local[*]")
conf.setAppName("TwitterStreamApp")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# set check point directory useful for recovery and stateful aggregations
sc.setCheckpointDir('checkpoint')
# initialize streaming context
ssc = StreamingContext(sc, 2)
# streaming source is socket
dataStream = ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
record_count = dataStream.map(lambda x: (x, 1))
# calculate the counts of tweets received till now
tags_totals = record_count.updateStateByKey(tweets_count)
tags_totals.pprint()
#Calculating the total tweets processed
countdf = tags_totals.map(lambda a:("count",a[1])).reduceByKey(lambda a,b:a+b)
countdf.pprint()
# calculating distinct tweets received till now
distinct_count = tags_totals.count()
distinct_count.pprint()
# process the stream data
processed_stream = dataStream.filter(lambda a: a != '')
processed_stream.pprint()
# Write the processed stream to a database
def sendPartition(iter):
    dbname = get_database()
    collection_name = dbname["TwitterData"]
    for record in iter:
        collection_name.insert_many([record])

# processed_stream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
