from streaming_service import kinesis_stream
from streaming_service import kinesis_producer
from streaming_service import kinesis_consumer
from alpha_vantage import generator

import argparse
import multiprocessing
import time
import data_config as dc


def trigger_producer(stream_name, partition_key, fx_generator):
    # Create and run producer. Need to also define generator function for run method.
    producer = kinesis_producer.kinesisProducer(stream_name, partition_key)
    producer.run(fx_generator.get_batch_fx_rate, ['AUD', 'EUR'])


def trigger_consumer(stream_name):
    # Create and run consumer
    print("On to consumer")
    consumer = kinesis_consumer.consumeData(stream_name, dc.SHARD_ID, dc.ITERATOR_TYPE)
    consumer.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-n', help='Stream name', required=True)
    parser.add_argument('-p', help='Partition key', required=True)
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    args = parser.parse_args()

    input_stream_name = args.n
    input_partition_key = args.p
    n_shards = int(args.s)
    aws_profile = args.u

    # Create kinesis stream
    if aws_profile:
        kinesis_stream = kinesis_stream.kinesisStream(input_stream_name, n_shards, aws_profile)
    else:
        kinesis_stream = kinesis_stream.kinesisStream(input_stream_name, n_shards)

    kinesis_stream.create_stream()

    prod = multiprocessing.Process(name='producer', target=trigger_producer, args=(input_stream_name, input_partition_key, generator.fxClient()))
    cons = multiprocessing.Process(name='consumer', target=trigger_consumer, args=(input_stream_name,))

    prod.start()
    time.sleep(dc.CONSUMER_STREAM_FREQ-1)
    cons.start()