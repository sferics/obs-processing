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
#regular expressions
import re
#for slicing dicts
from itertools import islice
from datetime import datetime as dt
#filesystem operation
from shutil import move

print( dt.now().strftime("%Y/%m/%d %H:%M") )

# Open database connection
db = MySQLdb.connect("localhost", "obs", "obs4data", "obs" )

# prepare a cursor object using cursor() method
cur = db.cursor()

known_stations  = []
station_params  = {}
obs_params      = {}

for i in ("station", "obs"):
    cur.execute("SHOW COLUMNS FROM %s WHERE field != 'updated'" % i)
    data = cur.fetchall()
    for j in data[1:]:
        eval(i+"_params")[ j[0] ] = None

bufr_dir      = "bufr/GMA/"
processed_dir = bufr_dir + "processed/"
files = glob( bufr_dir + "*---bin" )

#problematic = ("highwayDesignator", "stationOrSiteName", "stateOrFederalStateIdentifier")
problematic = ()
null_values = (2147483647, -1e+100, None, "XXXX")


clear  = lambda keyname : re.sub( r"#[0-9]+#", '', keyname )

number = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )

to_key = lambda number, clear_key : "#" + str(number) + "#" + clear_key


def sql_value_list(params, update=False):

    value_list = ""

    for i in params:
        if update:
            value_list += str(i) + " = "
        if params[i] in null_values:
            value_list += "NULL, "
        else:
            value_list += "'%s', " % str(params[i])

    return value_list[:-2]


def sql_values(params):

    column_list = ", ".join(params.keys())
    value_list  = sql_value_list(params)

    sql = "("+column_list+")" + " VALUES ("+value_list+")"

    return sql


def sql_insert(table, params, ignore=False, update=None):
    
    ignore = "IGNORE " if ignore else ""

    sql = "INSERT %sINTO %s " % (ignore, table)
    sql += sql_values(params)

    if update:
        params = dict( islice( obs_params.items(), update[0], update[1] ) )
        print("update: ", params)
        sql += " ON DUPLICATE KEY UPDATE "
        sql += sql_value_list( params, True )

    return sql


def sql_update(table, values):
    sql = "UPDATE %s WHERE" % table
    return sql


for FILE in files[:128]:
    with open(FILE, "rb") as f:
        
        bufr = ec.codes_bufr_new_from_file(f)
        try: ec.codes_set(bufr, "unpack", 1)
        except: continue
        try: iterid = ec.codes_bufr_keys_iterator_new(bufr)
        except: continue
        
        keys, nums = [], []

        while ec.codes_bufr_keys_iterator_next(iterid):
            
            #store keynames
            keyname = ec.codes_bufr_keys_iterator_get_name(iterid)
            clear_key = clear(keyname)
            
            if "#" in keyname and clear_key not in problematic:
                
                num       = number(keyname)

                if num not in nums:
                    nums.append(num)
                
                if clear_key not in keys:
                    keys.append(clear_key)
       
        #print(keys)

        for num in sorted(nums):
            
            cur.execute("SELECT DISTINCT shortStationName FROM station")
            data = cur.fetchall()

            for i in data:
                if i[0] not in known_stations:
                    known_stations.append(i[0])

            #print(known_stations)

            try:
                station_name = ec.codes_get( bufr, to_key(num, "shortStationName") )
            except:
                #print("No corresponding station!")
                continue
            
            if ( station_name not in known_stations ) and ( len(station_name) > 1 ):
                
                print(station_name)

                #add station to database
                for sp in station_params.keys():
                    
                    try:
                        value = ec.codes_get( bufr, to_key(num, sp) )
                        station_params[sp] = value
                    except:
                        continue
                
                print(station_params)
                sql = sql_insert( "station", station_params, ignore=True )
                #print(sql)

                #save station to db
                cur.execute( sql )
                db.commit()

            #get stationID by short station name
            try:
                sql = "SELECT stationID FROM station WHERE shortStationName='%s'" % station_name
                cur.execute(sql)
                stationID = cur.fetchone()[0]
                #print("stationID: %d" % stationID)
            except:
                continue
    
            obs_params["stationID"] = stationID

            #save obs
            for op in obs_params.keys():
                try:
                    value = ec.codes_get( bufr, to_key(num, op) )
                    obs_params[op] = value
                except:
                    continue
            
            #print(obs_params)

            #insert obsdata to db; on duplicate key update only obs values (airTemperature, ...)
            #sql = sql_insert( "obs", obs_params, update = ( 12, len(obs_params)-1 ) )
            
            #only save 15 min values
            if obs_params["timePeriod"] == -15:
                sql = sql_insert( "obs", obs_params, ignore = True )
                #print(sql)
                cur.execute( sql )
            
            db.commit()

        #for key in sorted(keys):
            #if clear_key =="timePeriod":
            #print( "keyname: %s" % key )
    
    #move file to processed folder
    #move( FILE, processed_dir + FILE.replace(bufr_dir, "") )


db.commit()

cur.close()

db.close()
