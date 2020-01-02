from multiprocessing import Process
import os

# https://medium.com/@bufan.zeng/use-parquet-for-big-data-storage-3b6292598653
# https://arrow.apache.org/docs/python/parquet.html
# https://xcalar.com/documentation/help/XD/1.4.0/Content/C_AdvancedTasks/M_Working%20with%20Parquet%20Files.htm

# IMPORTANT : https://stackoverflow.com/questions/45082832/how-to-read-partitioned-parquet-files-from-s3-using-pyarrow-in-python
                # https://stackoverflow.com/questions/49085686/pyarrow-s3fs-partition-by-timestamp

import pyarrow.parquet as pq
from multiprocessing import Process, cpu_count

if __name__ == '__main__':
    print(cpu_count())