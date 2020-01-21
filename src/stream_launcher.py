from streaming_service import kinesis_stream
from streaming_service import kinesis_producer
from streaming_service import kinesis_consumer
from alpha_vantage import generator

import argparse
import multiprocessing
import time, datetime
import data_config as dc
import signal

def trigger_producer(stream_name, partition_key, fx_generator, event):
    # Create and run producer. Need to also define generator function for run method.
    producer = kinesis_producer.kinesisProducer(stream_name, partition_key)
    producer.run(fx_generator.get_batch_fx_rate, dc.AV_FX_FROM, event)


def trigger_consumer(stream_name, event):
    # Create and run consumer
    consumer = kinesis_consumer.consumeData(stream_name, dc.SHARD_ID, dc.ITERATOR_TYPE)
    consumer.run(event)


def start_process(process_obj):
    if isinstance(process_obj, multiprocessing.context.Process):
        process_obj.start()
        print(process_obj.name + " process started at %s ." % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


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

    if kinesis_stream.create_stream():
        e = multiprocessing.Event()
        prod = multiprocessing.Process(name='producer', target=trigger_producer,
                                       args=(input_stream_name, input_partition_key, generator.fxClient(), e))
        cons = multiprocessing.Process(name='consumer', target=trigger_consumer, args=(input_stream_name, e))

        start_process(prod)
        time.sleep(5)
        start_process(cons)

        signal.signal(signal.SIGTERM, kinesis_stream.terminate_stream())
        prod.join(), cons.join()
        kinesis_stream.terminate_stream()

# python3.6 src/stream_launcher.py -n "test" -p  "test" -s 1
