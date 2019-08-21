# https://github.com/bufferapp/kiner/blob/master/kiner/producer.py
# https://github.com/awslabs/amazon-kinesis-client-python
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/examples.html
# MAIN: https://github.com/aws-samples/amazon-kinesis-learning/blob/learning-module-1/src/com/amazonaws/services/kinesis/samples/stocktrades/writer/StockTradesWriter.java
# READ: https://blog.sqreen.com/streaming-data-amazon-kinesis/
import boto3
import argparse

def validate_stream():
    return None




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-s', help='Required stream name', required=True)
    args = parser.parse_args()

    try:
        kinesis = boto3.client('kinesis')
        stream = kinesis.create_stream(StreamName=args.s, ShardCount=1)
        print('stream {} created'.format(args.s))

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