'''This script will collect tweets for a given searchterm within a defined timeframe
from twitter and store them into MongoDB.'''

import time
from datetime import datetime, timedelta
import logging
import pytz
import pymongo
import tweepy
import credentials

# Set timezone to UTC as required by twitter.
tz = pytz.timezone('UTC')

while True:

    # Create a connection to the MongoDB database server
    client = pymongo.MongoClient(host='mongodb') # hostname = servicename docker-compose pipeline

    # Create/use a database named twitter
    db = client.twitter

    # Define the collection
    collection = db.tweets

    # In bearer_token directly pass your bearer token for twitter
    # or better import it from a separate credentials file.
    tweet_client = tweepy.Client(
        bearer_token=credentials.BT,
        wait_on_rate_limit=True,
        )

    while True:
    # Set the timeinterval within which to search for tweets with searchterm
    # Default is 5 mins. When changing, adapt it in every part of the pipeline.
        time_now = datetime.now(tz)
        end_time = time_now - timedelta(seconds=10)
        start_time = end_time - timedelta(seconds=310)
        break

    # You can enter your searchterm here. Default is: politics
    search_query = "politics -is:retweet -is:reply -is:quote lang:en -has:links"

    # Get specific parts of tweets within given time interval. Default max = 1000.
    cursor = tweepy.Paginator(
        method=tweet_client.search_recent_tweets,
        query=search_query,
        end_time=end_time,
        start_time=start_time,
        tweet_fields=['author_id', 'created_at', 'public_metrics'],
    ).flatten(limit=1000)

    # Insert the tweets from cursor into MongoDB.
    for tweet in cursor:
        logging.warning('-----Tweet being written into MongoDB-----')
        logging.warning(tweet)
        collection.insert_one(dict(tweet))
        logging.warning(str(datetime.now()))
        logging.warning('----------\n')

    time.sleep(300)
