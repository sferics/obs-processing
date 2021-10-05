#!/venv/bin/python

import json
import pandas as pd
from glob import glob
import tarfile

bufr_dir      = "bufr/synop/"
processed_dir = bufr_dir + "processed/"
files = glob( bufr_dir + "*gz" )

for FILE in files:
    
    f = tarfile.open( FILE )

    j=json.decode(f)
    print(j)
