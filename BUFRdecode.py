#!/home/obs/miniconda3/bin
#decode bufr and save obs to database

from glob import glob        #file lookup
import eccodes as ec         #bufr decoder by ECMWF
from sqlite3 import connect  #python sqlite connector
import re, sys, os, psutil   #regular expressions, system, operating system, process handling
from pathlib import Path     #path operation
from datetime import datetime as dt
from functions import read_yaml, ts2dt, already_running, get_file_path, get_file_date
from database import db; db = db() #establish sqlite database connection (see database.py)

clear      = lambda keyname           : str( re.sub( r"#[0-9]+#", '', keyname ) )
number     = lambda keyname           : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
get_bufr   = lambda bufr, number, key : ec.codes_get( bufr, f"#{number}#{key}" )

#read config.yaml
config        = read_yaml( "config.yaml" )
station_info  = config["station_info"]
config_script = config["scripts"][sys.argv[0]]

null_vals     = set( config_script["null_vals"] + [None,-1e+100] )
time_keys     = config_script["time_keys"]
skip_keys     = config_script["skip_keys"]
skip_status   = config_script["skip_status"]
verbose       = config_script["verbose"]
profile       = config_script["profile"]
logging       = config_script["logging"]
conflict_keys = list(time_keys) + ["stID","prio"]

if profile: import cProfiler #TODO use module
if logging: import logging   #TODO use module

#parse command line arguments
if len(sys.argv) == 2:
    source = config["sources"][sys.argv[1]]
    
    if "," in source:
        sources = source.split(","); config_sources = {}
        for s in sources: config_sources[s] = config["sources"][s]
    
    else: config_sources = { sys.argv[1] : config["sources"][sys.argv[1]] }
else: config_sources = config["sources"]


def parse_all_bufrs( source, pid_file ):
    
    bufr_dir      = source + "/"
    config_source = config_sources[source]
    config_bufr   = config_source["bufr"]
    priority      = config_bufr["prio"]
    ext           = config_bufr["ext"]
    if type(ext) == list: ext = r"[" + "][".join(ext) + "]"
    multi_file    = config_bufr["multi_file"]

    skip_files     = set(db.files_status( skip_status, source ))
    files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + f"*.{ext}" )))
    files_to_parse = files_in_dir - skip_files

    if verbose:
        print("#FILES in DIR:  ", len(files_in_dir))
        print("#FILES in DB:   ", len(skip_files))
        print("#FILES to parse:", len(files_to_parse))

    Path(bufr_dir).mkdir(exist_ok=True)

    for FILE in files_to_parse:
        
        if multi_file: parsed_counter = 0
        skip_obs       = False
        source_name    = source[:]

        file_path = get_file_path( bufr_dir + FILE )

        if not db.file_exists( FILE, file_path ):
            file_date = get_file_date( file_path )
            #set file status = locked and get rowid (FILE ID)
            try: ID = db.register_file(FILE,file_path,source_name,status="locked",date=file_date,verbose=verbose)
            except Exception as e:
                if verbose: print(e)
                continue
        else:
            ID = db.get_file_id( FILE, file_path )
            #if file status is 'locked' continue with next file
            if db.get_file_status( ID ) == "locked": continue

        with open(bufr_dir + FILE, "rb") as f:
            try:
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None: db.set_file_status( ID, "empty", verbose=verbose ); continue
                ec.codes_set(bufr, "skipExtraKeyAttributes",  1)
                ec.codes_set(bufr, "unpack", 1)
                iterid = ec.codes_bufr_keys_iterator_new(bufr)
            except Exception as e:
                if verbose: print(e)
                db.set_file_status( ID, "error" ); continue
            
            keys = {}
            for skip_function in ( ec.codes_skip_duplicates, ec.codes_skip_computed, ec.codes_skip_function ):
                skip_function( iterid )

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

            ec.codes_keys_iterator_delete(iterid)

            if not multi_file: #workaround
                #BUFR messages all valid for one single station
                obs, meta = { "file" : ID, "prio" : priority }, {}
                for si in station_info:
                    try:                   meta[si] = ec.codes_get( bufr, si )
                    except Exception as e: meta[si] = None

                if meta["latitude"] in null_vals or meta["longitude"] in null_vals:
                    ec.codes_release(bufr); continue
                if meta["shortStationName"] not in null_vals and len(meta["shortStationName"]) == 4:
                    meta["stID"] = meta["shortStationName"]
                elif meta["stationNumber"] not in null_vals and meta["blockNumber"] not in null_vals:
                    meta["stID"] = str(meta["stationNumber"] + meta["blockNumber"]*1000).rjust(5, "0")
                else: ec.codes_release(bufr); continue

                if meta["stID"] in null_vals or meta["stationOrSiteName"] in null_vals:
                    ec.codes_release(bufr); continue
                
                if meta["stID"] not in db.known_stations():
                    meta["updated"] = dt.utcnow()
                    if verbose: print("Adding", meta["stationOrSiteName"], "to database...")
                    try: db.sql_insert( "station", meta )
                    except Exception as e:
                        if verbose: print(e)

            for num in keys:
                skip_obs = False

                if multi_file:
                    obs, meta = { "file" : ID, "prio" : priority }, {}
                    #TODO only update station info in dev mode, not operational!
                    for si in station_info:
                        try:    meta[si] = get_bufr( bufr, num, si )
                        except: meta[si] = "NULL"

                    if meta["latitude"] in null_vals or meta["longitude"] in null_vals:
                        continue
                    if meta["shortStationName"] not in null_vals and len(meta["shortStationName"]) == 4:
                        meta["stID"] = meta["shortStationName"] # for DWD nebenamtliche Stationen
                    elif meta["stationNumber"] not in null_vals and meta["blockNumber"] not in null_vals:
                        meta["stID"] = str(meta["stationNumber"] + meta["blockNumber"]*1000).rjust(5, "0")
                    else: continue

                    if meta["stID"] in null_vals or meta["stationOrSiteName"] in null_vals:
                        continue

                    if meta["stID"] not in db.known_stations():
                        meta["updated"] = dt.utcnow()
                        if verbose: print("Adding", meta["stationOrSiteName"], "to database...")
                        try: db.sql_insert( "station", meta )
                        except Exception as e:
                            if verbose: print(e)

                for key in keys[num]:
                    db.add_column( "obs", key )
                    #TODO: write param names and unit conversion dictionary
                    try:
                        obs[key] = get_bufr( bufr, num, key )
                        if obs[key] in null_vals: obs[key] = "NULL"
                    except Exception as e:
                        if verbose: print(f"{e}: {key}")
                        obs[key] = "NULL"

                obs["stID"]    = meta["stID"]
                obs["updated"] = dt.utcnow()

                #we need correct date/time information, otherwise skip this obs!
                if multi_file:
                    for tk in time_keys[:4]:
                        if tk not in obs or obs[tk] in null_vals:
                            skip_obs = True; break
                    if skip_obs: continue
                
                    #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
                    try: db.sql_insert( "obs", obs, conflict = conflict_keys, skip_update = conflict_keys )
                    except Exception as e:
                        if verbose: print(e)
                        db.set_file_status( ID, "error", verbose=verbose )
                    else:
                        db.set_file_status( ID, "parsed" )
                        parsed_counter += 1

            if multi_file:
                if parsed_counter == 0: db.set_file_status( ID, "empty", verbose=verbose )
            else:
                try:
                    start, stop = config_bufr["year"]
                    obs["year"] = FILE[start:stop]
                except: obs["year"] = dt.utcnow().year

                for tk in time_keys[:4]:
                    if tk not in obs or obs[tk] in null_vals:
                        skip_obs = True; break
                
                if skip_obs: ec.codes_release(bufr); continue

                #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
                try: db.sql_insert( "obs", obs, conflict = conflict_keys, skip_update = conflict_keys )
                except Exception as e:
                    if verbose: print(e)
                    db.set_file_status( ID, "error", verbose=verbose )
                else: db.set_file_status( ID, "parsed" )

            ec.codes_release(bufr) #release file to free memory
            db.commit() #force to commit changes to database

        memory_free = psutil.virtual_memory()[1] // 1024**2

        #if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            print("Too much RAM used, RESTARTING...")
            db.close()
            os.remove( pid_file )
            exe = sys.executable #restart program
            os.execl(exe, exe, * sys.argv); sys.exit()


if __name__ == "__main__":
    pid_file = config_script["pid_file"]

    if already_running( pid_file ):
        sys.exit( f"{sys.argv[0]} is already running... exiting!" )

    for SOURCE in config_sources:
        if verbose: print(f"Parsing source {SOURCE}...")
        parse_all_bufrs( SOURCE, pid_file )

    #commit to db and close all connections
    db.close()
    #remove file containing the pid, so the script can be started again
    os.remove( pid_file )
