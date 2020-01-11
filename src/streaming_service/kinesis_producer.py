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
import time, datetime
import threading
import pickle
import data_config as dc
from botocore.exceptions import ClientError


class kinesisProducer(threading.Thread):
    def __init__(self, stream_name, partition_key, stream_freq=dc.PRODUCER_STREAM_FREQ):
        super().__init__()
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.stream_freq = int(stream_freq)

    def put_record(self, data):
        self.client.put_record(StreamName=self.stream_name, Data=pickle.dumps(data), PartitionKey=self.partition_key)
        print("Put record at %s ." % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def run(self, generator_function, from_fx_pairs, event):
        """
        Generate list of prices per defined time frequency
        :param generator_function: lambda function i.e. stream_data(lambda: generator_function(#args))
        """
        while True:
            try:
                data = generator_function(from_fx_pairs)
                if data is not None: self.put_record(data)
                time.sleep(self.stream_freq)
            except self.client.exceptions.ResourceNotFoundException:
                event.set()
                print("Stream not found. Terminating processes")
                break
            except ClientError as e:
                print("Error occurred whilst streaming {}".format(e))
                continue



