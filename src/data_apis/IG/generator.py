import os
import sys
from pathlib import Path

cwd = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(str(cwd))
sys.path.append(str(cwd.parents[1]))

import itertools
import requests
from multiprocessing.pool import ThreadPool
import src.data_config as dc

# https://labs.ig.com/rest-trading-api-reference
# https://github.com/ig-python/ig-markets-api-python-library