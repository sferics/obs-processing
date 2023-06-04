#!/venv/bin/python

import json
import pandas as pd
from glob import glob
import compress_json
import gzip

bufr_dir      = "bufr/"
processed_dir = bufr_dir + "processed/"
files = glob( bufr_dir + "*.geojson.gz" )

station_info = ["stationNumber","blockNumber","stationOrSiteName","shortStationName"]
null_vals = (2147483647, -1e+100, None, "None", "XXXX", {}, "", [])


files=[bufr_dir+"Z__C_EDZW_20220730105700_bda01,synop_bufr_GER_999999_999999__MW_870.geojson.gz"]

for FILE in files:
    with gzip.open(FILE, 'r') as fin:
        data = dict(json.loads(fin.read().decode('utf-8')))

        data_keys = ['type', 'datetime_current', 'features']
        features = ['geometry', 'type', 'properties']

        for d in data:
            #print(data.keys())
            for t in data["type"]:
                print(t)

            for dtc in data["datetime_current"]:
                print(dtc)

            for f in data["features"]:
                #print(f.keys())
                for p in f["properties"].items():
                    try: p = dict(p[1])
                    except: continue
#                    print(p)
                    shortname = p["shortname"]
                    value = p["value"]
                    if shortname in station_info:
                        if shortname == station_info[0]:
                            stationNumber = value
                        elif shortname == station_info[1]:
                            blockNumber = value
                        elif shortname == station_info[2]:
                            stationOrSiteName = value
                        elif shortname == station_info[3]:
                            shortStationName = value
                        else: continue
                    else: continue

                print(blockNumber, stationNumber)
                if stationNumber not in null_vals:
                    if blockNumber not in null_vals:
                        stationID = str(stationNumber + blockNumber * 1000)
                        while len(stationID) < 5:
                            stationID = "0" +  stationID
                    else: stationID = "00" + station_number
                elif shortStationName not in null_vals:
                    stationID = "_" + shortStationName
                elif stationOrSiteName not in null_vals:
                    stationID = stationOrSiteName
                else: print("NO STATION INFO!")
                print(stationID)                   
