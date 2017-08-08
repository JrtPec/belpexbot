from time import sleep
import pandas as pd
from entsoe import Entsoe
import tweepy
import datetime as dt

from settings import entsoe_api_key, twitter_key, twitter_secret, twitter_token, twitter_token_secret, log_account

auth = tweepy.OAuthHandler(twitter_key, twitter_secret)
auth.set_access_token(twitter_token, twitter_token_secret)

twitter_api = tweepy.API(auth)


def get_day_ahead():
    tomorrow = (pd.Timestamp.utcnow().tz_convert(tz='Europe/Brussels') + dt.timedelta(days=1)).replace(hour=0)
    client = Entsoe(api_key=entsoe_api_key)
    day_ahead = client.query_price(country_code='BE', start=tomorrow, end=tomorrow + dt.timedelta(days=1),
                                   as_series=True)
    if day_ahead is not None:
        day_ahead = day_ahead.tz_convert('Europe/Brussels')
    return day_ahead


def send_success_dm():
    now = pd.Timestamp.now(tz='Europe/Brussels')
    message = "It is {}, and day ahead values were fetched successfully".format(now.strftime('%c'))
    twitter_api.send_direct_message(log_account, text=message)


def send_tweet(tweet):
    if len(tweet) > 140:
        raise ValueError('Tweet too long!')
    twitter_api.update_status(tweet)


def tweetgen(negatives):
    for time, val in negatives.iteritems():
        tweet = "Tomorrow ({}) negative electricity prices: {} €/MWh from {} to {}h! #BelPEX".format(
            time.strftime('%-d %b'),
            str(val).replace('.', ','),
            time.strftime('%-H'),
            (time + dt.timedelta(hours=1)).strftime('%-H')
        )
        yield tweet


def run():
    while True:
        day_ahead = get_day_ahead()
        if day_ahead is not None:
            send_success_dm()
            break
        else:
            sleep(600)

    negatives = day_ahead[day_ahead < 0]

    tweets = tweetgen(negatives)

    for tweet in tweets:
        send_tweet(tweet)

if __name__ == '__main__':
    run()
