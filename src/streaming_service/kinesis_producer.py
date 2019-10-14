# https://blog.sqreen.com/streaming-data-amazon-kinesis/
# https://medium.com/data-alchemist/how-to-collect-data-for-cryptocurrency-algorithmic-trading-and-what-to-collect-f944d4b1a69

# Storage Options
# https://aws.amazon.com/timestream/
# https://stackoverflow.com/questions/9815234/how-to-store-7-3-billion-rows-of-market-data-optimized-to-be-read
# https://ericdraken.com/storing-stock-candle-data-efficiently/
# https://ericdraken.com/api-challenges-java/
# http://discretelogics.com/teafiles/#teafilespython

# REMEMBER THIS HAS TO BE AGNOSTIC TO SOURCE (i.e AV OR IG ETC)
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import time
import datetime
import threading
import data_config as dc


class kinesisProducer(threading.Thread):
    def __init__(self, stream_name, partition_key, stream_freq=dc.PRODUCER_STREAM_FREQ):
        super().__init__()
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.stream_freq = int(stream_freq)

    def put_record(self, data):
        self.client.put_record(StreamName=self.stream_name, Data=data, PartitionKey=self.partition_key)

    def run(self, generator_function, from_fx_pairs):
        """
        Generate list of prices per defined time frequency
        :param generator_function: lambda function i.e. stream_data(lambda: generator_function(#args))
        """
        # consider batch method here
        # https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream

        while True:
            try:
                print(datetime.datetime.now())
                data = generator_function(from_fx_pairs)
                self.put_record(data)
                time.sleep(self.stream_freq)
            except Exception as e:
                print("Error occurred whilst streaming {}".format(e))
                continue


