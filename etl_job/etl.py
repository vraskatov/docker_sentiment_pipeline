'''This script will clean tweets with regular expressions, analyze their sentiments
and insert them into a PostgreSQL database.'''

import re
from datetime import datetime, timedelta
import time
import pymongo
import pytz
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Establish a connection to the MongoDB server.
client = pymongo.MongoClient(host="mongodb", port=27017)

time.sleep(100)  # seconds

# Select the database (twitter) you want to use withing the MongoDB server.
db = client.twitter

pg = create_engine('postgresql://docker_user:1234@postgresdb:5432/twitter', echo=True)
pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    text VARCHAR(500),
    sentiment NUMERIC,
    date TIMESTAMP
);
''')

def clean_tweets(tweet):
    '''A function that removes mentions, hashtags, RT for retweets, many URLs
    as well as some (markdown) emojis.'''
    mentions_regex = r'@[A-Za-z0-9_]+'
    url_regex = r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])'
    # regex from: https://stackoverflow.com/questions/40251930/
    # matching-regular-expression-for-urls-in-javascript-produces-null
    # License: https://creativecommons.org/licenses/by-sa/3.0/
    hashtag_regex = '#[A-Za-z0-9_]+'
    rt_regex = r'RT\s'
    emojis_regex = r'[:][a-zÄäÖöÜüß]+\w?[a-zÄäÖöÜüß]+?\w?[a-zÄäÖöÜüß]+?[:]'
    tweet = re.sub(mentions_regex, '', tweet)
    tweet = re.sub(hashtag_regex, '', tweet)
    tweet = re.sub(rt_regex, '', tweet)
    tweet = re.sub(url_regex, '', tweet)
    tweet = re.sub(emojis_regex, '', tweet)

    return tweet

# Set timezone to UTC.
tz = pytz.timezone('UTC')

while True:

    analyser = SentimentIntensityAnalyzer()
    time_now = datetime.now(tz)
    end_time = time_now - timedelta(seconds=130)
    start_time = end_time - timedelta(seconds=430)

    # Search for tweets within given timeframe.
    docs = db.tweets.find({"created_at": {"$gte": start_time, "$lt": end_time}})

    # Calculate the sentiment for the tweets and insert them into postgres.
    for doc in docs:
        text = doc['text']
        text = clean_tweets(text)
        score = analyser.polarity_scores(text)['compound']
        date = doc['created_at']
        query = "INSERT INTO tweets VALUES (%s, %s, %s);"
        pg.execute(query, (text, score, date))

    time.sleep(300)
    