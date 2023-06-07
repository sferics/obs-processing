#/!venv/bin/python3

#decode bufr and save obs to database

#file lookup
from glob import glob
#import numpy as np
#import pandas as pd
#bufr decoder by ECMWF
import eccodes as ec
#python MySQL connector
import MySQLdb
#regular expressions, sys and os
import re, sys, os
#for slicing dicts
from itertools import islice
from datetime import datetime as dt
#filesystem operation
from shutil import move
from pathlib import Path

print( dt.now().strftime("%Y/%m/%d %H:%M") )

# Open database connection
db = MySQLdb.connect("localhost", "obs", "obs4data", "obs" )
# prepare a cursor object using cursor() method
cur = db.cursor()

#read sql files to create db tables and execute the SQL statements
station_sql = Path("station.sql").read_text()
cur.execute( station_sql )
obs_sql = Path("obs.sql").read_text()
cur.execute( obs_sql )

bufr_dir      = "bufr/"
processed_dir = bufr_dir + "processed/"
files = glob( bufr_dir + "*.bin" )

skip      = ["unexpandedDescriptors", "timeIncrement"]
station_info = Path("station_info.txt").read_text().split("\n")[:-1]
timekeys = ['year', 'month', 'day', 'hour', 'minute', 'timePeriod', 'timeSignificance']
null_vals = (2147483647, -1e+100, None, "None", "null", "NULL", "MISSING", "XXXX", {}, "", [], ())

clear  = lambda keyname : re.sub( r"#[0-9]+#", '', keyname )
number = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key = lambda number, clear_key : "#" + str(number) + "#" + clear_key


def sql_value_list(params, update=False):

    value_list = ""

    for i in params:
        print("PARAM:",i)
        if update:
            value_list += str(i) + " = "
        if params[i] in null_vals:
            value_list += "NULL, "
        else:
            value_list += "'%s', " % str(params[i])

    return value_list[:-2]


def sql_values(params):

    column_list = ", ".join(params.keys())
    value_list  = sql_value_list(params)
    sql = f"({column_list}) VALUES ({value_list})"

    return sql


def sql_insert(table, params, ignore=False, update=None):
    
    ignore = "IGNORE " if ignore else ""
    sql = f"INSERT {ignore}INTO {table} "
    sql += sql_values(params)

    if update:
        params = dict( islice( params.items(), update[0], update[1] ) )
        sql += " ON DUPLICATE KEY UPDATE "
        sql += sql_value_list( params, True )

    return sql


sql_update = lambda table, SET, WHERE : r"UPDATE {table} SET {SET} WHERE {WHERE}"
get        = lambda bufr, num, key : ec.codes_get( bufr, to_key(num, key) )


known_stations  = []

for FILE in files:
    with open(FILE, "rb") as f:
        try:
            bufr = ec.codes_bufr_new_from_file(f)
            if bufr is None: continue
            ec.codes_set(bufr, "unpack", 1)
            iterid = ec.codes_bufr_keys_iterator_new(bufr)
        except: continue
        
        keys, obs_keys, nums = ([] for _ in range(3))

        while ec.codes_bufr_keys_iterator_next(iterid):
            #store keynames
            keyname = ec.codes_bufr_keys_iterator_get_name(iterid)
            clear_key = clear(keyname)

            if "#" in keyname and clear_key not in skip:
                num = number(keyname)
                if num not in nums: nums.append(num)
                if clear_key not in keys:
                    keys.append(clear_key)
                    if clear_key not in station_info:
                        obs_keys.append(clear_key)

        for num in sorted(nums): #get known stations
            cur.execute("SELECT DISTINCT `stationID` FROM `station`")
            data = cur.fetchall()

            for i in data:
                if i[0] not in known_stations: known_stations.append(i[0])

            obs, station_meta = {}, {}

            for si in station_info:
                try: station_meta[si] = get( bufr, num, si )
                except: station_meta[si] = None

            if station_meta["shortStationName"]:
                stationID = str(station_meta["shortStationName"])
            else:
                station_number = station_meta["stationNumber"]
                block_number   = station_meta["blockNumber"]
                if station_number not in null_vals and block_number not in null_vals:
                    stationID = str(station_number + block_number * 1000).rjust(5,"0")
                else: continue

            station_meta["stationID"] = stationID
            station_name = station_meta["stationOrSiteName"]

            if ( stationID not in known_stations ) and ( len(station_name) > 0 ):
                print("Adding", station_name, "to database...")
                sql = sql_insert( "station", station_meta, ignore=True )
                #save station to db
                try: cur.execute( sql ); db.commit()
                except: continue

            #save obs
            for key in obs_keys:
                #max identifier length in mysql is 64! TODO:
                #write conversion dictionary!
                key = key[:64]
                try: cur.execute( f"ALTER TABLE obs ADD COLUMN {key} VARCHAR(255)" ); db.commit()
                except: pass
                try: obs[key] = get( bufr, num, key )
                except: continue

            #insert obsdata to db; on duplicate key update only obs values (airTemperature, ...)
            sql = sql_insert( "obs", obs, update = ( 12, len(obs)-1 ) )
            try: cur.execute( sql )
            except: continue
    
    #move file to processed folder
    move( FILE, processed_dir + FILE.replace(bufr_dir, "") )


db.commit()
cur.close()
db.close()
