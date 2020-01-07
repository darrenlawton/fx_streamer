import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# https://medium.com/@bufan.zeng/use-parquet-for-big-data-storage-3b6292598653
# https://arrow.apache.org/docs/python/parquet.html
# https://xcalar.com/documentation/help/XD/1.4.0/Content/C_AdvancedTasks/M_Working%20with%20Parquet%20Files.htm

# IMPORTANT : https://stackoverflow.com/questions/45082832/how-to-read-partitioned-parquet-files-from-s3-using-pyarrow-in-python
# https://stackoverflow.com/questions/49085686/pyarrow-s3fs-partition-by-timestamp

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
# Continued writing: https://github.com/apache/arrow/issues/3203

from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

dict_writer = {}


def write_to_parquet(dict_blob):
    print(dict_blob)
    table = dict_to_table(dict_blob)
    writer = get_writer(dict_blob, table)
    if writer:
        writer.write_table(table=table)
        print("Written to parquet file.")


def dict_to_table(dict_blob):
    """ Convert dictionary to table

    :param: data blob as dict
    :return: a parquet table
    """
    df = transform_to_df(dict_blob)
    if df is not None:
        return pa.Table.from_pandas(df)


def transform_to_df(dict_blob):
    if isinstance(dict_blob, dict):
        # Convert dict to df
        df = pd.DataFrame.from_dict(dict_blob, orient='index').transpose()
        df['6. Last Refreshed'] = pd.to_datetime(df['6. Last Refreshed'])
        df = df.set_index('6. Last Refreshed')
        return df
    else:
        return None


def register_writer(file_name, dtable):
    # need to register in dict, and remove anything already there
    # close existing writer
    global dict_writer
    writer = pq.ParquetWriter(file_name + '.parquet', dtable.schema)
    dict_writer[file_name] = writer
    print(file_name + " now registered")
    return writer


def deregister_writer(file_name, old_writer):
    global dict_writer
    old_writer.close()
    try:
        del dict_writer[file_name]
        print(file_name + " now deregistered")
    except:
        print(file_name + " could not remove from the writer registry.")


def get_file_name(fx_pair, refresh_date):
    return fx_pair + "_" + format(refresh_date, '%d%m%Y')


def get_writer(dict_blob, table):
    global dict_writer
    writer = None

    # dict keys naming convention: fxpair_date, value will be writer object
    if isinstance(dict_blob, dict):
        fx_pair = dict_blob['1. From_Currency Code']
        refresh_date = datetime.strptime(dict_blob['6. Last Refreshed'], '%Y-%m-%d %H:%M:%S')

        key_list = [*dict_writer]
        writer_key = next((f for f in key_list if fx_pair in f), None)
        print("checking writer")
        if writer_key:
            print("found writer key")
            writer_date = datetime.strptime(writer_key.split('_')[1], '%d%m%Y')
            print("writer date: " + str(writer_date.date()))
            print("refresh date: " + str(refresh_date.date()))

            if writer_date.date() == refresh_date.date():
                writer = dict_writer[writer_key]
            elif writer_date.date() < refresh_date.date():
                deregister_writer(writer_key, dict_writer[writer_key])
                writer = register_writer(get_file_name(fx_pair, refresh_date),table)
        else:
            # create whole new writer
            writer = register_writer(get_file_name(fx_pair, refresh_date), table)

    return writer


def process_date():
    # if existing date (if any), doesn't match blob, then close file.
    raise NotImplementedError


if __name__ == '__main__':
    t = pq.read_table('EUR_07012020.parquet')
    p = t.to_pandas()
    print(p)

