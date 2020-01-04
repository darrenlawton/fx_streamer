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


from multiprocessing import Process, cpu_count
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

writer = None

def dict_to_table(dict_blob):
    """Convert dictionary to table

    :param blob: Data blob as dict
    :return: Parquet table
    """
    if isinstance(dict_blob, dict):
        # return pa.Table.from_pandas(pd.DataFrame.from_dict(dict_blob).transpose())
        t = pa.Table.from_pandas(pd.DataFrame.from_dict(dict_blob).transpose())
        writer = write_to_parquet(writer, t)
    else: None


def write_to_parquet(writer, table):
    if writer is None:
         writer = pq.ParquetWriter('test.parquet', table.schema)
    writer.write_table(table=table)
    return writer


if __name__ == '__main__':
    t = pq.read_table('test.parquet')
    p = t.to_pandas()
    print(p)