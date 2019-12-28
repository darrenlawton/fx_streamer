# https://towardsdatascience.com/delivering-real-time-streaming-data-to-amazon-s3-using-amazon-kinesis-data-firehose-2cda5c4d1efe
# https://towardsdatascience.com/turn-amazon-s3-into-a-spatio-temporal-database-40f1a210e943

# https://ericdraken.com/comparison-time-series-data-transport-formats/
# https://acadgild.com/blog/parquet-file-format-hadoop


## CURRENT TABS OPEN ###
# We going to use parquet - https://arrow.apache.org/docs/python/parquet.html
# CONSUMER https://blog.sqreen.com/streaming-data-amazon-kinesis/
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.get_records
# https://docs.aws.amazon.com/streams/latest/dev/tutorial-stock-data-kplkcl-consumer.html
# https://github.com/aws-samples/amazon-kinesis-learning/tree/learning-module-1/src/com/amazonaws/services/kinesis/samples/stocktrades/processor
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import time, datetime
import pickle
import data_config as dc


class kinesisConsumer:
    def __init__(self, stream_name, shard_id, iterator, stream_freq=dc.CONSUMER_STREAM_FREQ):
        super().__init__()
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.iterator = iterator
        self.stream_freq = stream_freq

    @staticmethod
    def iterate_records(records):
        for r in records:
            partition_key = r['PartitionKey']
            data = pickle.loads(r['Data'])

        yield partition_key, data

    def run(self):
        """
        Poll stream for new record and pass to processing method
        """
        response = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                  ShardId=self.shard_id,
                                                  ShardIteratorType=self.iterator)
        iteration = response['ShardIterator']
        start = datetime.datetime.now()
        end = start + datetime.timedelta(seconds=self.stream_freq)

        while True:
            try:
                response = self.client.get_records(ShardIterator=iteration)
                records = response['Records']
                if records:
                    self.process_records(records)

                iteration = response['NextShardIterator']

                time.sleep(self.stream_freq)

            except Exception as e:
                print("Error occurred whilst consuming stream {}".format(e))
                time.sleep(1)


class consumeData(kinesisConsumer):
    def process_records(self, records):
        for partition_key, data_blob in self.iterate_records(records):
            print(partition_key, ":", data_blob)

        # Error
        # occurred
        # whilst
        # consuming
        # stream
        # An
        # error
        # occurred(ExpiredIteratorException)
        # when
        # calling
        # the
        # GetRecords
        # operation: Iterator
        # expired.The
        # iterator
        # was
        # created
        # at
        # time
        # Sun
        # Dec
        # 0
        # 8
        # 11: 40:31
        # UTC
        # 2019
        # while right now it is Sun Dec 08 11:45: 40
        # UTC
        # 2019
        # which is further in the
        # future
        # than
        # the
        # tolerated
        # delay
        # of
        # 300000
        # milliseconds.

        # Deserialise record

        # Ensure each blob is a list type

        # for part_key, data in self.iter_records(records):
            # print(part_key, ":", data)