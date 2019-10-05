# https://blog.sqreen.com/streaming-data-amazon-kinesis/
# https://medium.com/data-alchemist/how-to-collect-data-for-cryptocurrency-algorithmic-trading-and-what-to-collect-f944d4b1a69

# Storage Options
# https://aws.amazon.com/timestream/
# https://stackoverflow.com/questions/9815234/how-to-store-7-3-billion-rows-of-market-data-optimized-to-be-read
# https://ericdraken.com/storing-stock-candle-data-efficiently/
# https://ericdraken.com/api-challenges-java/
# http://discretelogics.com/teafiles/#teafilespython

# REMEMBER THIS HAS TO BE AGNOSTIC TO SOURCE (i.e AV OR IG ETC)

import time
import threading


class kinesisProducer(threading.Thread):
    def __init__(self, kinesis_client, stream_name, partition_key, stream_freq):
        super().__init__()
        self.client = kinesis_client
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.stream_freq = stream_freq

    def put_record(self, data):
        self.client.put_record(self.stream_name, data, self.partition_key)

    def run(self, generator_function):
        """
        Generate list of prices per defined time frequency
        :param generator_function: lambda function i.e. stream_data(lambda: generator_function(#args))
        """
        while True:
            try:
                data = generator_function()
                self.put_record(data)
                time.sleep(self.stream_freq)
            except Exception as e:
                print("Error occurred whilst streaming {}".format(e))
                continue


