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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

writer = None
dict_writer = {}


def dict_to_table(dict_blob):
    """ Convert dictionary to table

    :param: Data blob as dict
    :return: A parquet table
    """
    global writer, test_count

    df = transform_to_df(dict_blob)

    if df is not None:
        table = pa.Table.from_pandas(df)
        # writer = write_to_parquet(writer, table)
        test_count += 1
        if test_count > 6:
            writer.close()
            print("writer closed")


def write_to_parquet(writer, table):
    if writer is None:
        writer = pq.ParquetWriter('test.parquet', table.schema)
    writer.write_table(table=table)
    return writer


def transform_to_df(dict_blob):
    if isinstance(dict_blob, dict):
        # Convert dict to df
        df = pd.DataFrame.from_dict(dict_blob).transpose()
        df['6. Last Refreshed'] = pd.to_datetime(df['6. Last Refreshed'])
        df = df.set_index('6. Last Refreshed')
        return df
    else:
        return None


def get_writer(df_blob):
    global dict_writer
    return writer


def get_file_name():
    raise NotImplementedError


if __name__ == '__main__':
    t = pq.read_table('test.parquet')
    p = t.to_pandas()
    p['6. Last Refreshed'] = pd.to_datetime(p['6. Last Refreshed'])
    p = p.set_index('6. Last Refreshed')
    print(p.columns)
