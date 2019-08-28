
# https://github.com/bufferapp/kiner/blob/master/kiner/producer.py
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
# MAIN: https://github.com/aws-samples/amazon-kinesis-learning/blob/learning-module-1/src/com/amazonaws/services/kinesis/samples/stocktrades/writer/StockTradesWriter.java
# ACCOMPANIED: https://blog.sqreen.com/streaming-data-amazon-kinesis/
# https://blog.sqreen.com/streaming-data-amazon-kinesis/
# https://www.influxdata.com/products/influxdb-overview/

import boto3
import argparse, os, time
from src.data import data_config as dc


class kinesisStream():
    def __init__(self, stream_name, n_shards, aws_profile = 'default'):
        self.stream_name = stream_name
        self.n_shards = n_shards
        os.environ['AWS_PROFILE'] = aws_profile
        self.client = boto3.client('kinesis')

    def create_stream(self):
        try:
            self.client.create_stream(StreamName=stream_name, ShardCount=n_shards)
            self.validate_stream
        except self.client.exceptions as e:
            print("Unable to create kinesis stream: %s" % e)

    def terminate_stream(self):
        try:
            self.client.delete_stream(StreamName=stream_name)
        except self.client.exceptions as e:
            print("Unable to delete kinesis stream: %s" % e)

    def validate_stream(self):
        status = None
        while status != dc.VALID_STREAM:
            try:
                response = self.client.describe_stream(StreamName=self.stream_name)
                status = response.get('StreamDescription').get('StreamStatus')
                time.sleep(1)
            except self.client.exceptions as e:
                print("Error found while describing the stream: %s" % e)

        print('kinesis stream active {} '.format(self.stream_name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-n', help='Stream name', required=True)
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    args = parser.parse_args()

    stream_name = args.n
    n_shards = args.s
    aws_profile = args.u

    if aws_profile:
        kinesis_stream = kinesisStream(stream_name, n_shards, aws_profile)
    else:
        kinesis_stream = kinesisStream(stream_name, n_shards)
