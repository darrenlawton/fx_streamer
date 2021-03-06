
# https://github.com/bufferapp/kiner/blob/master/kiner/producer.py
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
# MAIN: https://github.com/aws-samples/amazon-kinesis-learning/blob/learning-module-1/src/com/amazonaws/services/kinesis/samples/stocktrades/writer/StockTradesWriter.java
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import os, time
import data_config as dc
from botocore.exceptions import ClientError
from data_store import writer


class kinesisStream():
    def __init__(self, stream_name, n_shards, aws_profile='default'):
        self.stream_name = stream_name
        self.n_shards = n_shards

        try:
            os.environ['AWS_PROFILE'] = aws_profile
            self.client = boto3.client('kinesis')
        except:
            print("Error, could not configure AWS profile.")
            sys.exit()

    def create_stream(self):
        try:
            self.client.create_stream(StreamName=self.stream_name, ShardCount=self.n_shards)
        except self.client.exceptions.ResourceInUseException:
            print('stream {} already exists.'.format(self.stream_name))
            pass
        except ClientError as e:
            print('Unable to create kinesis stream: {}'.format(e))

        return self.validate_stream()

    def terminate_stream(self):
        try:
            writer.deregister_all()
            self.client.delete_stream(StreamName=self.stream_name)
        except ClientError as e:
            print("Unable to delete kinesis stream: {}".format(e))

    def validate_stream(self):
        status = ""

        while status != dc.VALID_STREAM:
            print(status)
            try:
                response = self.client.describe_stream(StreamName=self.stream_name)
                status = response.get('StreamDescription').get('StreamStatus')
                time.sleep(1)
            except ClientError as e:
                print("Error found while describing the stream: %s" % e)
                return False

        # Enable enhanced monitoring, if flag set in data_config
        if dc.ENHANCED_MONITORING:
            response = self.client.enable_enhanced_monitoring(StreamName=self.stream_name,
                                                              ShardLevelMetrics=dc.SHARD_LVL_METRICS)

        print('kinesis stream active {} '.format(self.stream_name))

        return True

