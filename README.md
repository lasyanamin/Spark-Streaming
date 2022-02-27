# Spark-Streaming
main.py reads data from twitter search APi and sends data over tcp connection. 
streaming.py reads data from main.py using spark streaming . Counts the total number of tweets processed, counts number of distinct tweets, writes tweets to mongo db.
