import os
import sys
import inspect

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_filename():
    return inspect.getframeinfo(inspect.currentframe()).filename


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
import data_store.s3_interface as s3
import data_config as dc

global dict_writer
dict_writer = {}


def write_to_parquet(dict_blob):
    try:
        table = dict_to_table(dict_blob)
        writer = get_writer(dict_blob, table)
        if writer:
            writer.write_table(table=table)
    except:
        deregister_all
        print("parquet file writer exception.")
        pass


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


def register_writer(new_key, dtable):
    writer = pq.ParquetWriter(get_local_store(new_key), dtable.schema)
    dict_writer[new_key] = writer
    print(new_key + " now registered")
    return writer


def deregister_writer(key):
    # close parquet writer
    dict_writer[key].close()

    # transfer parquet file from local FS to S3
    fx_pair = file_date = key.split('_')[0]
    file_date = datetime.strptime(key.split('_')[1], '%d%m%Y')
    if s3.upload_file(get_local_store(key), dc.BUCKET,
                      s3.get_object_name(key, fx_pair, file_date)):
        os.remove(get_local_store(key))

    # remove writer from dictionary registry
    try:
        del dict_writer[key]
        print(key + " now deregistered")
    except:
        print(key + " could not remove from the writer registry.")
        pass


def deregister_all():
    [deregister_writer(key) for key in dict_writer.keys()]


def get_local_store(key):
    return dc.get_file_path(get_filename()) + '/local_temp/' + key + '.parquet'


def get_file_name(fx_pair, refresh_date):
    return fx_pair + "_" + format(refresh_date, '%d%m%Y')


def get_writer(dict_blob, table):
    writer = None

    # dict keys naming convention: fxpair_date, value will be writer object
    if isinstance(dict_blob, dict):
        fx_pair = dict_blob['1. From_Currency Code']
        refresh_date = datetime.strptime(dict_blob['6. Last Refreshed'], '%Y-%m-%d %H:%M:%S')

        key_list = [*dict_writer]
        writer_key = next((f for f in key_list if fx_pair in f), None)
        if writer_key:
            writer_date = datetime.strptime(writer_key.split('_')[1], '%d%m%Y')

            if writer_date.date() == refresh_date.date():
                writer = dict_writer[writer_key]
            elif writer_date.date() < refresh_date.date():
                deregister_writer(writer_key)
                writer = register_writer(get_file_name(fx_pair, refresh_date), table)
        else:
            # create new writer
            writer = register_writer(get_file_name(fx_pair, refresh_date), table)

    return writer


if __name__ == '__main__':
    print(dc.get_file_path(get_filename()))
    t = pq.read_table(dc.get_file_path(get_filename()) + '/local_temp/AUD_11012020.parquet', use_threads=True)
    p = t.to_pandas()
    print(p)
