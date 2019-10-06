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

import boto3
import time
import pyarrow.parquet as pq

class kinesisConsumer:
    def __init__(self, stream_name, shard_id, iterator, stream_freq):
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
            data = r['Data']

        yield partition_key, data

    def run(self):
        """
        Poll stream for new record and pass to processing method
        """
        response = self.client.get_shard_iterator(self.stream_name, self.shard_id, self.iterator)
        iteration = response['ShardIterator']

        while True:
            try:
                response = self.kinesis_client(iteration, limit=60)
                records = response['Records']
                if records:
                    self.process_records(records)
                iteration = response['NextShardIterator']
                time.sleep(self.stream_freq)
            except Exception as e:
                print("Error occurred whilst consuming stream {}".format(e))


class consumeData(kinesisConsumer):
    raise NotImplemented