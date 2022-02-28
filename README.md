# Spark-Streaming

### Built With

* python
* pyspark

### Prerequisites

1. Register at twitter apps to get access to Twitter API
2. apache spark 
3. Mongo DB

## Description 
1. main.py reads data from twitter search APi and sends data over tcp connection. 
2. streaming.py reads data from main.py using spark streaming . Counts the total number of tweets processed, counts number of distinct tweets, writes tweets to mongo db.
