# https://docs.aws.amazon.com/streams/latest/dev/tutorial-stock-data-kplkcl.html
# https://github.com/bufferapp/kiner/blob/master/kiner/producer.py
# https://github.com/awslabs/amazon-kinesis-client-python
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/examples.html
# MAIN: https://github.com/aws-samples/amazon-kinesis-learning/blob/learning-module-1/src/com/amazonaws/services/kinesis/samples/stocktrades/writer/StockTradesWriter.java
# READ: https://blog.sqreen.com/streaming-data-amazon-kinesis/

import boto3
import argparse, os, time

VALID_STREAM = 'ACTIVE'


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
        while status != VALID_STREAM:
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


# try:
#     client = boto3.client('kinesis',region_name="us-east-2")
#     response = client.create_stream(StreamName='TwitterStream',ShardCount=1)
#
#     print('stream {} created'.format(stream_name))
# except ResourceInUseException:
#     print('stream {} already exists'.format(stream_name))
#     client.delete_stream(StreamName='TwitterStream')
#
# status = 'not set'
# while( status != 'ACTIVE' )
#     describe_stream_response = client.describe_stream(stream_name)
#     description = describe_stream_response.get('StreamDescription')
#     status = description.get('StreamStatus')
#     time.sleep(1)
#
# api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
#
# kinesis = boto3.client('kinesis')