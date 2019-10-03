# https://towardsdatascience.com/delivering-real-time-streaming-data-to-amazon-s3-using-amazon-kinesis-data-firehose-2cda5c4d1efe
# https://towardsdatascience.com/turn-amazon-s3-into-a-spatio-temporal-database-40f1a210e943

# https://ericdraken.com/comparison-time-series-data-transport-formats/
# https://acadgild.com/blog/parquet-file-format-hadoop

# We going to use parquet - https://arrow.apache.org/docs/python/parquet.html
# CONSUMER https://blog.sqreen.com/streaming-data-amazon-kinesis/

import pyarrow.parquet as pq

class kinesisConsumer:
    def __init__(self, kinesis_client, stream_name, shard_id, stream_freq, iterator):
        super().__init__()
        self.client = kinesis_client
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.stream_freq = stream_freq
        self.iterator = iterator

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
        response = self.client.get_shard_iterator(self.stream_name, self.shard_id)

        try:
            return None
        except Exception as e:
            print("Error occurred whilst consuming stream {}".format(e))


class dataWriter(kinesisConsumer):
    raise NotImplemented