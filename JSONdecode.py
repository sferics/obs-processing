#!/venv/bin/python

import os, json, bz2, compress_json
import pandas as pd
from glob import glob
import numpy as np

bufr_dir      = "bufr/"
processed_dir = bufr_dir + "processed/"
files = glob( bufr_dir + "*bz2" )

for FILE in files:
    #os.rename( FILE, FILE[:-1] )
    #JSON = compress_json.load(FILE[:-1])
    """
    with bz2.open( FILE, "rt" ) as f:
        j = json.loads(f)
        print(j)
    """
    with bz2.open(FILE, "rb") as bz:
        bz = bz.read()
        JSON = json.loads(bz)
        print(JSON)
        keys_all = []
        for j in JSON:
            print(j)
            keys = []
            key = np.copy(j)
            if key not in keys_all:
                keys_all.append(key)
            if key not in keys:
                keys.append(key)
            print(keys)
