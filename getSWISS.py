#!/bin/python3

import json, sys, os, shutil
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta as td, timezone as tz
from database import db; db = db()

fmt  = "%Y-%m-%d@%H:%M"
fmt2 = "%Y%m%d%H%M"
latest_name = "VQHA80"
C = ".csv"
path = "SWISS/"

#create path directory if not exists
from pathlib import Path
Path(path).mkdir(parents=True, exist_ok=True)

if len(sys.argv) == 2:
   if sys.argv[1] == "-a":
      #take all .csv
      outfiles = glob(path + "*.csv")
      print(outfiles)
   else:
      outfiles = [path + sys.argv[1] + C]
else:
   # download latest csv to SWISS folder as "YYYY-MM-DD@hh:mm.csv" and "VQHA80.csv"
   import wget
   filename = latest_name + C
   url = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/"

   #remove oldest tmp file
   try: os.remove(path + filename)
   except FileNotFoundError: print("No latest file, downloading new!")
   #download latest file and copy it
   outfiles = [wget.download( url + filename, out = path )]
   shutil.copyfile( outfiles[0], path + dt.utcnow().strftime(fmt + C) )

stations = {
        6712 : "AIG",
        6672 : "ALT",
        6601 : "BAS",
        6633 : "BUS",
        6608 : "CHM",
        6786 : "CHU",
        6717 : "GSB",
        6700 : "GVE",
        6728 : "GRC",
        6730 : "JUN",
        6760 : "OTL",
        6770 : "LUG",
        6637 : "MER",
        6610 : "PAY",
        6794 : "ROB",
        6792 : "SAM",
        6720 : "SIO",
        6680 : "SAE",
        6781 : "TAV",
        6745 : "ULR",
        6990 : "VAD",
        6660 : "SMA",
        6670 : "KLO"
        }

# param_name in csv => param_name in db
params = { "rrr10":"rre150z0", "sun10":"sre000z0" }

for outfile in outfiles:

   # now we can work with the downloaded file (extract observations)
   df = pd.read_csv(outfile, sep=";")

   #add SQL columns
   for p in list(params.keys()):
      db.add_column( "obs", p )

   obs = {}

   for s in stations:
      data = df.loc[df['Station/Location'] == stations[s]]
      for p in params:
         try:
            value = float(data[params[p]].iloc[0])
            value = int(np.round(value))
         except (ValueError,IndexError):
            value = 'NULL'
         try:
            Date = str(int(data["Date"].iloc[0]))
         except: continue
         datum = Date[:8]
         year  = datum[:4]
         month = datum[4:6]
         day   = datum[6:8]
         stdmin = Date[8:]
         hour = stdmin[:2]
         minute = stdmin[2:]

         obs[p] = value

      db.sql_insert( "obs", obs, conflict=("stID","year","month","day","hour","minute") ) 
   
   # commit and close db
   db.commit(); db.close()
