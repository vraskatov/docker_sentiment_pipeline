'''This script will make two Postgres queries: one for the most positive tweet
and one for the most negative one, pick one tweet per category
and post such tweets in a slack channel on an hourly basis.'''

import time
from datetime import timedelta
from sqlalchemy import create_engine
import pandas as pd
import requests
import credentials

time.sleep(240)
pg = create_engine('postgresql://docker_user:1234@postgresdb:5432/twitter', echo=True)

# Create a Webhook object to connect to Slack.
# Pass your webhook directly into webhook_url or load it from a credentials file.
webhook_url = credentials.WH

while True:
    try:
        # The query to get the best score tweet:
        query_best = f"""
        SELECT text, sentiment, date
        FROM tweets
        WHERE date > LOCALTIMESTAMP - INTERVAL '9 minutes'
        ORDER BY sentiment DESC LIMIT 1
        """
        # The query to get the worst score tweet:
        query_worst = f"""
        SELECT text, sentiment, date
        FROM tweets
        WHERE date > LOCALTIMESTAMP - INTERVAL '9 minutes'
        ORDER BY sentiment LIMIT 1
        """

        # Reading query and write to variable with pandas.
        tweet_best = pd.read_sql_query(query_best, con=pg)
        tweet_worst = pd.read_sql_query(query_worst, con=pg)

        # Get value of text, sentiment and date.
        text_tweet_best = tweet_best['text'].iloc[0]
        sentiment_tweet_best = tweet_best['sentiment'].iloc[0]
        date_tweet_best = tweet_best['date'].iloc[0] + timedelta(hours=1)
        text_tweet_worst = tweet_worst['text'].iloc[0]
        sentiment_tweet_worst = tweet_worst['sentiment'].iloc[0]
        date_tweet_worst = tweet_worst['date'].iloc[0] + timedelta(hours=1)

        # Create the JSON data object.
        data = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*The Best Tweet Received*\n:+1: With a Score of {sentiment_tweet_best}\nWas posted on {date_tweet_best}\n{text_tweet_best}"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*The Worst Tweet Received*\n:-1: With a Score of {sentiment_tweet_worst}\nWas posted on {date_tweet_worst}\n{text_tweet_worst}"
                    }
                }
            ]
        }

        # Post the data to the Slack channel.
        requests.post(url=webhook_url, json=data)
    except IndexError:
        print('Something is asynchronous here…')
    time.sleep(300)
