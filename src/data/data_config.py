import os, sys

# Alpha Vantage
AV_HOST = 'www.alphavantage.co'
AV_API_KEY = os.getenv('ALPHA_VANTAGE_KEY')


def validate_key(api_key):
    if api_key is None:
        print("Alpha Vantage API key not set as ENV variable.")
        sys.exit()
    else: return api_key

# AWS Kinesis Stream


# Autoencoder
