import socket
import sys
import requests
import requests_oauthlib
import json
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/recent?query=cat%20has%3Amedia%20-grumpy&tweet.fields=created_at",
        auth=my_auth, stream=True)
    print("https://api.twitter.com/2/tweets/search/stream/rules", response)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    full_tweet = http_resp.content
    data = json.loads(full_tweet)['data']
    while(1):
        for line in data:
            try:
                tweet_text = line['text']
                print("Tweet Text: " + tweet_text)
                print ("------------------------------------------")
                tcp_connection.send((tweet_text + '\n').encode('utf-8'))
            except:
                e = sys.exc_info()[0]
                print("Error: %s" % e)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    TCP_IP = "localhost"
    TCP_PORT = 9009
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    resp = get_tweets()
    send_tweets_to_spark(resp, conn)

