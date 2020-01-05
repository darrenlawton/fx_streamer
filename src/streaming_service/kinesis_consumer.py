# https://towardsdatascience.com/delivering-real-time-streaming-data-to-amazon-s3-using-amazon-kinesis-data-firehose-2cda5c4d1efe
# https://towardsdatascience.com/turn-amazon-s3-into-a-spatio-temporal-database-40f1a210e943

# https://ericdraken.com/comparison-time-series-data-transport-formats/
# https://acadgild.com/blog/parquet-file-format-hadoop

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
import data_store.writer as ds
from botocore.exceptions import ClientError

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

    def run(self, event):
        """
        Poll stream for new record and pass to processing method
        """
        response = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                  ShardId=self.shard_id,
                                                  ShardIteratorType=self.iterator)
        iteration = response['ShardIterator']

        while not event.is_set():
            try:
                response = self.client.get_records(ShardIterator=iteration)
                records = response['Records']
                if records:
                    self.process_records(records)

                iteration = response['NextShardIterator']

                time.sleep(self.stream_freq)

            except ClientError as e:
                print("Error occurred whilst consuming stream {}".format(e))
                time.sleep(1)

        print("Consumer terminated.")


class consumeData(kinesisConsumer):
    def process_records(self, records):
        for partition_key, data_blob in self.iterate_records(records):
            [ds.dict_to_table(i) for i in data_blob]
