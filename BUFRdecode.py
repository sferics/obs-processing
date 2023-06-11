#/!venv/bin/python3
#decode bufr and save obs to database

from glob import glob        #file lookup
import eccodes as ec         #bufr decoder by ECMWF
from sqlite3 import connect  #python sqlite connector
import re, sys, os, psutil   #regular expressions, system, operating system, process handling
from pathlib import Path     #path operation
from datetime import datetime as dt
from functions import read_yaml
from database import db; db = db()

clear      = lambda keyname           : str( re.sub( r"#[0-9]+#", '', keyname ) )
number     = lambda keyname           : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
get_bufr   = lambda bufr, number, key : ec.codes_get( bufr, f"#{number}#{key}" )

#read config.yaml
config        = read_yaml( "config.yaml" )
priority      = config["priorities"]["bufr"]
station_info  = config["station_info"]
config_script = config["scripts"][sys.argv[0]]

null_vals     = set( config_script["null_vals"] + [None] )
time_keys     = config_script["time_keys"]
skip_keys     = config_script["skip_keys"]
skip_status   = config_script["skip_status"]
multi_file    = config_script["multi_file"]
verbose       = config_script["verbose"]
profile       = config_script["profile"]
logging       = config_script["logging"]

if profile: import cProfiler #TODO use module
if logging: import logging   #TODO use module


if len(sys.argv) == 2:
    source = config["sources"][sys.argv[1]]
    
    if "," in source:
        sources = source.split(","); config_sources = {}
        for s in sources: config_sources[s] = config["sources"][s]
    
    else: config_sources = { sys.argv[1] : config["sources"][sys.argv[1]] }
else: config_sources = config["sources"]


def parse_all_bufrs( source ):
    bufr_dir      = source + "/"
    config_source = config_sources[source]
    ext           = config_source["bufr"]["ext"]
    if type(ext) == list:
        ext = r"[" + "][".join(ext) + "]"
        print(ext)

    skip_files     = set(db.files_status( skip_status ))
    files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + f"*.{ext}" )))
    files_to_parse = files_in_dir - skip_files

    if verbose:
        print("#FILES in DIR:  ", len(files_in_dir))
        print("#FILES in DB:   ", len(skip_files))
        print("#FILES to parse:", len(files_to_parse))

    Path(bufr_dir).mkdir(exist_ok=True)

    for FILE in files_to_parse:
        parsed_counter = 0
        skip_obs       = False
        station0       = False
        source_name    = source[:]

        #if file status is 'locked' continue with next file
        if db.get_file_status( FILE, source_name ) == "locked": continue

        if source == "DWD":
            if FILE[-29:-26] == "GER":  source_name += "_ger"
            else:                       source_name += "_int"
        
        file_path      = str( Path( bufr_dir + FILE ).resolve().parent )
        #set file status = locked and get rowid (FILE ID)
        ID = db.register_file( FILE, file_path, source_name )

        with open(bufr_dir + FILE, "rb") as f:
            try:
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    db.set_file_status( FILE, "empty", verbose=verbose )
                    continue
                ec.codes_set(bufr, "skipExtraKeyAttributes",  1)
                ec.codes_set(bufr, "unpack", 1)
                iterid = ec.codes_bufr_keys_iterator_new(bufr)
            except Exception as e:
                if verbose: print(e)
                db.set_file_status( FILE, "error" )
                continue
            
            keys = {}

            while ec.codes_bufr_keys_iterator_next(iterid):
                keyname   = ec.codes_bufr_keys_iterator_get_name(iterid)
                clear_key = clear(keyname)
                try: num = number(keyname)
                except: continue

                if "->" in keyname: continue #associated field
                elif "#" in keyname:
                    if clear_key not in list(skip_keys) + list(station_info):
                        try:    keys[number(keyname)].add( clear_key )
                        except: keys[number(keyname)] = set()
                elif not multi_file:
                    try:    keys[0].add( keyname )
                    except: keys[0] = set()

            if source not in multi_file: #workaround
                #BUFR messages all valid for one single station
                obs, meta = { "file" : ID, "priority" : priority }, {}
                for si in station_info:
                    try:                   meta[si] = ec.codes_get( bufr, si )
                    except Exception as e: meta[si] = None
                try: del keys[0]
                except: pass

            for num in keys:
                skip_obs = False

                if source in multi_file:
                    obs, meta = { "file" : ID, "priority" : priority }, {}
                    #TODO only update station info in dev mode, not operational!
                    for si in station_info:
                        try:                   meta[si] = get_bufr( bufr, num, si )
                        except Exception as e: meta[si] = None

                if meta["latitude"] in null_vals or meta["longitude"] in null_vals:
                    continue
                if meta["shortStationName"] not in null_vals and len(meta["shortStationName"]) == 4:
                    meta["stID"] = meta["shortStationName"]
                elif meta["stationNumber"] not in null_vals and meta["blockNumber"] not in null_vals:
                    meta["stID"] = str(meta["stationNumber"] + meta["blockNumber"]*1000).rjust(5, "0")
                else: continue

                if meta["stID"] in null_vals or meta["stationOrSiteName"] in null_vals: continue

                if (meta["stID"] not in db.known_stations()) and (len(meta["stationOrSiteName"]) > 1):
                    meta["updated"] = dt.utcnow()
                    if verbose: print("Adding", meta["stationOrSiteName"], "to database...")
                    try: db.cur.execute( db.sql_insert( "station", meta ) )
                    except Exception as e:
                        if verbose: print(e)

                for key in keys[num]:
                    #if skip_obs == True: break
                    try:    db.cur.execute(f'ALTER TABLE obs ADD COLUMN "{key[:64]}"')
                    except: pass
                    #max length of mysql identifier is 64!
                    #TODO: write param names and unit conversion dictionary
                    try: value = get_bufr( bufr, num, key )
                    except Exception as e:
                        if verbose: print(f"{e}: {key}")
                    if value in null_vals: value = None
                    obs[key[:64]] = value

                obs["stID"]    = meta["stID"]
                obs["updated"] = dt.utcnow()

                #we need correct date/time information, otherwise skip this obs!
                if source in multi_file:
                    for tk in time_keys[:4]:
                        if tk not in obs or obs[tk] in null_vals:
                            skip_obs = True; break
                    if skip_obs: continue
                
                    #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
                    conflict = "stID, " + ", ".join(time_keys) 
                    sql = db.sql_insert( "obs", obs, conflict=conflict, skip_update=list(time_keys)+["stID"] )
                    try:
                        db.cur.execute( sql )
                        db.set_file_status( FILE, "parsed" )
                        parsed_counter += 1
                    except Exception as e:
                        if verbose: print(e)
                        db.set_file_status( FILE, "error" )

            if source in multi_file:
                if parsed_counter == 0:
                    db.set_file_status( FILE, "empty" )
            else:
                if source == "RMI":     obs["year"] = FILE[11:15]
                elif source == "COD":   obs["year"] = FILE[0:2]
                else:                   obs["year"] = dt.utcnow().year
                #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
                conflict = "stID, " + ", ".join(time_keys)
                sql = db.sql_insert( "obs", obs, conflict=conflict, skip_update=list(time_keys)+["stID"] )
                try:
                    db.cur.execute( sql )
                    db.set_file_status( FILE, "parsed" )
                except Exception as e:
                    if verbose: print(e)
                    db.set_file_status( FILE, "error" )

        db.commit()
        ec.codes_release(bufr) #release file to free memory
        process     = psutil.Process(os.getpid())
        memory_used = process.memory_info().rss  // 1024**2
        memory_free = psutil.virtual_memory()[1] // 1024**2

        #TODO: remove this nasty workaround after memory leak is fixed!
        #if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            print("Too much RAM used, RESTARTING...")
            db.close()
            exe = sys.executable #restart program
            os.execl(exe, exe, * sys.argv); sys.exit()

for SOURCE in config_sources:
    if verbose: print(f"Parsing source {SOURCE}...")
    parse_all_bufrs( SOURCE )

#commit to db and close all connections
db.close()
