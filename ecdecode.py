#/!venv/bin/python3
#decode bufr and save obs to database

from glob import glob       #file lookup
import eccodes as ec        #bufr decoder by ECMWF
from sqlite3 import connect #python sqlite connector
import re, sys, os          #regular expressions, system and operating system
from shutil import move     #moving/copying files
from pathlib import Path    #path operation
from datetime import datetime as dt

db  = connect("obs.db")   # Creating obs db and opening database connection
cur = db.cursor()                                       # Creating cursor object to call SQL
#read sql files to create db tables and execute the SQL statements
read_file = lambda file_name : Path( file_name ).read_text()
for table in ("station", "obs"): cur.execute( read_file( table + ".sqlite" ) )

bufr_dir      = "bufr/"
processed_dir = bufr_dir + "processed/"
error_dir     = bufr_dir + "error/"
Path(processed_dir[:-1]).mkdir(parents=True, exist_ok=True)
Path(error_dir[:-1]).mkdir(exist_ok=True)
skip          = ["unexpandedDescriptors", "timeIncrement"]
station_info  = [ _ for _ in read_file( "station_info.txt" )[:-1].splitlines() ]
time_keys     = ['year', 'month', 'day', 'hour', 'minute', 'timePeriod', 'timeSignificance']
null_vals     = (2147483647,-1e+100,None,"None","null","NULL","MISSING","XXXX",{},"",[],(),set())
skip_obs      = False

clear      = lambda keyname           : re.sub( r"#[0-9]+#", '', keyname )
number     = lambda keyname           : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key     = lambda number, clear_key : "#" + str(number) + "#" + clear_key
get_bufr   = lambda bufr, num, key    : ec.codes_get( bufr, to_key(num, key) )
sql_update = lambda table, SET, WHERE : r"UPDATE {table} SET {SET} WHERE {WHERE}"

def sql_value_list(params, update=False):
    value_list = ""
    for i in params:
        if update:                 value_list += '"' + str(i) + '"' + " = "
        if params[i] in null_vals: value_list += "NULL, "
        else: value_list += '"%s", ' % str(params[i])
    return value_list[:-2]

def sql_values(params):
    column_list = '"' + '", "'.join(params.keys()) + '"'
    value_list  = sql_value_list(params)
    return f"({column_list}) VALUES ({value_list})"

def sql_insert(table, params, update = None, skip_update = () ):
    sql = f"INSERT INTO {table} " + sql_values(params)
    if update:
        for i in skip_update:
            try:    params.pop(i)
            except: continue
        sql += f" ON CONFLICT({update}) DO UPDATE SET "+sql_value_list(params,True)
    return sql

def known_stations():
    cur.execute( "SELECT DISTINCT stID FROM station" )
    data = cur.fetchall()
    if data: return (i[0] for i in data)
    else:    return ()


for FILE in glob( bufr_dir + "*.bin" ): #get list of files in bufr_dir
    with open(FILE, "rb") as f:
        try:
            bufr = ec.codes_bufr_new_from_file(f)
            if bufr is None:
                move( FILE, error_dir + FILE.replace(bufr_dir, "") )
                print("BUFR is NONE, moved file")
                continue
            ec.codes_set(bufr, "skipExtraKeyAttributes",  1)
            ec.codes_set(bufr, "unpack", 1)
            iterid = ec.codes_bufr_keys_iterator_new(bufr)
        except Exception as e:
            print(e)
            move( FILE, error_dir + FILE.replace(bufr_dir, "") )
            continue
        
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
                try: meta[si] = get_bufr( bufr, num, si )
                except Exception as e:
                    print(e)
                    meta[si] = None
            
            if meta["shortStationName"]: meta["stID"] = str(meta["shortStationName"])
            else:
                if meta["stationNumber"] not in null_vals and meta["blockNumber"] not in null_vals:
                    meta["stID"] = str(meta["stationNumber"] + meta["blockNumber"]*1000).rjust(5, "0")
                else: continue
            
            if (meta["stID"] not in known_stations()) and (len(meta["stationOrSiteName"]) > 1):
                meta["updated"] = dt.utcnow()
                print("Adding", meta["stationOrSiteName"], "to database...")
                try:
                    cur.execute( sql_insert( "station", meta ) )
                    db.commit()
                except Exception as e:
                    print(e)
            
            for key in obs_keys:
                if skip_obs == True: break
                #TODO better use PRAGMA table_info(obs) statement here
                #https://stackoverflow.com/questions/3604310/alter-table-add-column-if-not-exists-in-sqlite
                try:    cur.execute(f'ALTER TABLE obs ADD COLUMN "{key}"'); db.commit()
                except: pass
                #max length of mysql identifier is 64!
                #TODO: write param names and unit conversion dictionary
                try:    obs[key] = get_bufr( bufr, num, key[:64] )
                except Exception as e:
                    print(e)
                    move( FILE, error_dir + FILE.replace(bufr_dir, "") )
                    skip_obs = True

            if skip_obs == True: continue
            obs["stID"]    = meta["stID"]
            obs["updated"] = dt.utcnow()

            #we need correct date/time information, otherwise skip this obs!
            for tk in time_keys[:5]:
                if tk not in obs or obs[tk] in null_vals:
                    skip_obs = True; break
            if skip_obs: continue
            
            #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
            update = "stID," + ",".join(time_keys)
            sql = sql_insert( "obs", obs, update = update, skip_update = time_keys + ["stID"] )
            try: cur.execute( sql )
            except Exception as e:
                print(e)
                move( FILE, error_dir + FILE.replace(bufr_dir, "") )
                continue
           
    ec.codes_release(bufr)                                      #release file to free memory
    try: move( FILE, processed_dir + FILE.replace(bufr_dir, "") )    #move FILE to the "processed" folder
    except: continue

db.commit(); cur.close(); db.close()
