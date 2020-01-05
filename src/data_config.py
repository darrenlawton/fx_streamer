import os
import sys

# Alpha Vantage
AV_HOST = 'https://www.alphavantage.co/query'
AV_API_KEY = os.getenv('PREM_ALPHA_VANTAGE_KEY')
AV_FUNCTION_CALL = "CURRENCY_EXCHANGE_RATE" # This API returns the realtime exchange rate for any pair of digital currency (e.g., Bitcoin) and physical currency (e.g., USD). Data returned for physical currency (Forex) pairs also include realtime bid and ask prices.
AV_FX_FROM = ['AUD', 'GBP', 'EUR']


def validate_key(api_key):
    if api_key is None:
        print("Alpha Vantage API key not set as ENV variable.")
        sys.exit()
    else:
        return api_key


# AWS Kinesis Stream
VALID_STREAM = 'ACTIVE'
SHARD_ID = 'shardId-000000000000'
ITERATOR_TYPE = 'TRIM_HORIZON'
PRODUCER_STREAM_FREQ = 60 * 2.5 # shard iterator expires after 5 minutes
CONSUMER_STREAM_FREQ = PRODUCER_STREAM_FREQ


# AWS S3
BUCKET = 'lawtoninvestmentco'


# Autoencoder
