#/!venv/bin/python3
#decode bufr and save obs to database

from glob import glob       #file lookup
import eccodes as ec        #bufr decoder by ECMWF
from MySQLdb import connect #python MySQL connector
import re, sys, os          #regular expressions, system and operating system
from shutil import move     #moving/copying files
from pathlib import Path    #path operation

db  = connect("localhost", "obs", "obs4data", "obs" )   # Opening database connection
cur = db.cursor()                                       # Creating cursor object to call SQL
#read sql files to create db tables and execute the SQL statements
read_file = lambda file_name : Path( file_name ).read_text()
for table in ("station", "obs"): cur.execute( read_file( table + ".sql" ) )

#constants
bufr_dir      = "bufr/"
processed_dir = bufr_dir + "processed/"
skip          = ["unexpandedDescriptors", "timeIncrement"]
station_info  = [ _ for _ in read_file( "station_info.txt" )[:-1].splitlines() ]
time_keys     = ['year', 'month', 'day', 'hour', 'minute', 'timePeriod', 'timeSignificance']
null_vals     = (2147483647, -1e+100, None, "None", "null", "NULL", "MISSING", "XXXX", {}, "", [], ())

clear      = lambda keyname : re.sub( r"#[0-9]+#", '', keyname )
number     = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key     = lambda number, clear_key : "#" + str(number) + "#" + clear_key
get_bufr   = lambda bufr, num, key : ec.codes_get( bufr, to_key(num, key) )
sql_update = lambda table, SET, WHERE : r"UPDATE {table} SET {SET} WHERE {WHERE}"

def sql_value_list(params, update=False):
    value_list = ""
    for i in params:
        if update: value_list += str(i) + " = "
        if params[i] in null_vals: value_list += "NULL, "
        else: value_list += "'%s', " % str(params[i])
    return value_list[:-2]

def sql_values(params):
    column_list = ", ".join(params.keys())
    value_list  = sql_value_list(params)
    return f"({column_list}) VALUES ({value_list})"

def sql_insert(table, params, ignore = False, update = None, skip_update = () ):
    ignore = "IGNORE " if ignore else ""
    sql = f"INSERT {ignore}INTO {table} " + sql_values(params)
    if update:
        for i in skip_update:
            try:    params.pop(i)
            except: continue
        sql += " ON DUPLICATE KEY UPDATE " + sql_value_list( params, True )
    return sql

def known_stations():
    cur.execute("SELECT DISTINCT `stationID` FROM `station`")
    data = cur.fetchall()
    if data:    return (i[0] for i in data)
    else:       return ()


for FILE in glob( bufr_dir + "*.bin" ): #get list of files in bufr_dir
    with open(FILE, "rb") as f:
        try:
            bufr = ec.codes_bufr_new_from_file(f)
            if bufr is None: continue
            ec.codes_set(bufr, "unpack", 1)
            iterid = ec.codes_bufr_keys_iterator_new(bufr)
        except: continue
        
        keys, obs_keys, nums = [], [], []

        while ec.codes_bufr_keys_iterator_next(iterid):
            
            keyname = ec.codes_bufr_keys_iterator_get_name(iterid)
            clear_key = clear(keyname)

            if "#" in keyname and clear_key not in skip:
                num = number(keyname)
                if num not in nums: nums.append(num)
                if clear_key not in keys:
                    keys.append(clear_key)
                    if clear_key not in station_info:
                        obs_keys.append(clear_key)

        for num in sorted(nums):
            obs, meta = {}, {}
            for si in station_info:
                try:    meta[si] = get_bufr( bufr, num, si )
                except: meta[si] = None

            if meta["shortStationName"]: stationID = str(meta["shortStationName"])
            else:
                if meta["stationNumber"] not in null_vals and meta["blockNumber"] not in null_vals:
                    stationID = str(meta["stationNumber"] + meta["blockNumber"] * 1000).rjust(5,"0")
                else: continue
            meta["stationID"], obs["stationID"] = stationID, stationID
            
            if ( stationID not in known_stations() ) and ( len(meta["stationOrSiteName"]) > 1 ):
                print("Adding", meta["stationOrSiteName"], "to database...")
                sql = sql_insert( "station", meta, ignore=True )
                try: cur.execute( sql ); db.commit()
                except: continue

            for key in obs_keys:
                try: cur.execute( f"ALTER TABLE obs ADD COLUMN {key} VARCHAR(255)" ); db.commit()
                except: pass
                #max length of mysql identifier is 64! TODO: write conversion dictionary
                try: obs[key] = get_bufr( bufr, num, key[:64] )
                except: continue

            #insert obsdata to db; on duplicate key update only obs values; no stationID or time_keys
            sql = sql_insert( "obs", obs, update = True, skip_update = time_keys + ["stationID"] )
            try: cur.execute( sql )
            except: continue
    
    move( FILE, processed_dir + FILE.replace(bufr_dir, "") )    #move FILE to the "processed" folder
    ec.codes_release(bufr)                                      #release file to free memory

db.commit()
cur.close(); db.close()
