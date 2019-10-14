from streaming_service import kinesis_stream
from streaming_service import kinesis_producer
from streaming_service import kinesis_consumer
from alpha_vantage import generator

import argparse
import data_config as dc

# threading: https://dzone.com/articles/python-thread-part-1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-n', help='Stream name', required=True)
    parser.add_argument('-p', help='Partition key', required=True)
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    args = parser.parse_args()

    stream_name = args.n
    partition_key = args.p
    n_shards = int(args.s)
    aws_profile = args.u

    # Create kinesis stream
    if aws_profile:
        kinesis_stream = kinesis_stream.kinesisStream(stream_name, n_shards, aws_profile)
    else:
        kinesis_stream = kinesis_stream.kinesisStream(stream_name, n_shards)

    kinesis_stream.create_stream()

    # Create and run producer. Need to also define generator function for run method.
    producer = kinesis_producer.kinesisProducer(stream_name, partition_key)
    generator = generator.fxClient()
    producer.run(generator.get_batch_fx_rate, ['AUD', 'EUR'])

    # Create and run consumer
    print("on to consumer")
    consumer = kinesis_consumer.kinesisConsumer(stream_name, dc.SHARD_ID, dc.ITERATOR_TYPE)
    try:
        consumer.run()
    finally:
        kinesis_stream.terminate_stream()
