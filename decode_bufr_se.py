#/usr/bin/env !python
# decodes BUFRs for availabe or given sources and saves obs to database

import argparse, sqlite3, random, time, re, sys, os, psutil, shelve
import logging as log
from copy import copy
import random, time
import numpy as np
from glob import glob 
import eccodes as ec        # bufr decoder by ECMWF
from pathlib import Path    # path operation
from datetime import datetime as dt, timedelta as td
from database import database; import global_functions as gf; import global_variables as gv
from bufr_functions import clear, to_datetime, convert_keys_se

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible


def parse_all_BUFRs( source=None, file=None, known_stations=None, pid_file=None ):
    #TODO
    """
    Parameter:
    ----------
    source : name of source (str)
    pid_file : name of the file where the process id gets stored (str)

    Notes:
    ------
    main function of the script, parses all files of a given source and tries to save them in database
    using the obs_to_station_db() function. includes file handling and sets the status of a file to
    'locked' before starting to handle it, to 'empty' if no (relevant) data was found in it, to status
    'error' if something went wrong (which should not occur but we never know...) or - if everything
    went smooth to status == 'parsed' in the file_table of the main database. pid_file is optional

    Return:
    -------
    None
    """
    if source:
        config_source   = config_sources[source]
        if "bufr" in config_source: config_bufr = config_source["bufr"]
        else: return
        bufr_dir        = config_bufr["dir"] + "/"
        #priority        = config_bufr["prio"]
        ext             = config_bufr["ext"]
        """
        if "tables" in config_bufr:
            old_path = ec.codes_definition_path() 
            os.putenv('ECCODES_DEFINITION_PATH', config_bufr["tables"] + ":" + old_path)
        """
        try:    clusters = set(config_source["clusters"].split(","))
        except: clusters = None

        db = database(db_file, timeout=timeout_db, traceback=traceback)
        
        for i in range(max_retries):
            try:    known_stations = db.get_stations( clusters )
            except: pass
            else:   break
        
        if i == max_retries - 1: sys.exit(f"Can't access main database, tried {max_retries} times. Is it locked?")

        if "glob" in config_bufr and config_bufr["glob"]:   ext = f"{config_bufr['glob']}.{ext}"
        else:                                               ext = f"*.{ext}" #TODO add multiple extensions (list)
        
        files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + ext )))

        if args.redo:   skip_files  = set()
        else:           skip_files  = set(db.get_files_with_status( config_script["skip_status"], source ))

        files_to_parse = list( files_in_dir - skip_files )
        
        #TODO special sort functions for CCA, RRA and stuff...
        #TODO implement order by datetime of files
        if config_script["sort_files"]: files_to_parse = sorted(files_to_parse)
        if config_script["max_files"]:  files_to_parse = files_to_parse[:config_script["max_files"]]

        if verbose:
            print("#FILES in DIR:  ",   len(files_in_dir))
            print("#FILES to skip: ",   len(skip_files))
            print("#FILES to parse:",   len(files_to_parse))

        gf.create_dir( bufr_dir )
        
        file_IDs = {}

        for FILE in files_to_parse:
            file_path = gf.get_file_path( bufr_dir + FILE )
            file_date = gf.get_file_date( file_path )
            if args.redo:
                ID = db.get_file_id(FILE, file_path)
                if not ID: ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)
                file_IDs[FILE] = ID
            else:
                file_IDs[FILE] = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)
        db.close(commit=True)

        #TODO if multiprocessing: split file_to_parse by number of processes (eg 8) and parse files simultaneously
    
    elif file:
        FILE            = file.split("/")[-1]
        files_to_parse  = (FILE,)
        file_path       = gf.get_file_path(args.file)
        file_date       = gf.get_file_date(args.file)
        bufr_dir        = "/".join(file.split("/")[:-1]) + "/"
        source          = args.extra # default: extra

        db = database(db_file, timeout=timeout_db, traceback=traceback)
        known_stations  = db.get_stations()

        ID = db.get_file_id(FILE, file_path)
        if ID:  db.set_file_status(ID,"locked")
        else:   ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)

        db.close(commit=True)

        file_IDs = {FILE:ID}

    obs, file_statuses = {}, set()
    file_statuses = set()

    for FILE in files_to_parse:
        if debug: print(bufr_dir + FILE) 
        with open(bufr_dir + FILE, "rb") as f:
            try:
                ID = file_IDs[FILE]
                # if for whatever reason no ID (database lock?) or filestatus means skip: continue with next file
                if not ID: continue
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    file_statuses.add( ("empty", ID) )
                    if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                ec.codes_set(bufr, 'skipExtraKeyAttributes', 1)
                ec.codes_set(bufr, "unpack", 1)
                #ec.codes_set(bufr, "doExtractSubsets", 1)
            except Exception as e:
                log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                if verbose: print(log_str)                
                if traceback: gf.print_trace(e)
                file_statuses.add( ("error", ID) )
                continue
            else:
                product_kind = ec.CODES_PRODUCT_BUFR
                if debug: print(product_kind)
                if product_kind not in {0,1,2,6,7}: continue
                obs[ID] = {} #shelve.open(f"shelves/{ID}", writeback=True) #{}
                 
            iterid = ec.codes_bufr_keys_iterator_new(bufr)
            #if debug: print(ec.codes_get_message(bufr))
            
            for fun in (ec.codes_skip_computed, ec.codes_skip_function, ec.codes_skip_duplicates): fun(iterid)

            meta, typical   = {}, {}
            valid_obs       = False
            location        = None
            skip_next       = 10
            subset, new_obs = 0, 0
            skip_obs        = False
            last_key        = None

            if debug: pdb.set_trace()

            #obs = []

            while ec.codes_bufr_keys_iterator_next(iterid):
                #if skip_next: skip_next -= 1; continue
                
                key = ec.codes_bufr_keys_iterator_get_name(iterid)
               
                if debug:
                    try:    value = ec.codes_get( bufr, key )
                    except: print(key, ec.codes_get_array(bufr,key))
                    obs.append((key,value)); continue
                
                if last_key == "typical" and last_key not in key:
                    last_key = None; skip_next = 3; continue

                if key == "subsetNumber":
                    if subset > 0:
                        meta = {}; location = None; valid_obs = False; skip_obs = False
                    subset += 1; continue
                elif skip_obs: continue

                clear_key = clear(key)
                if clear_key not in relevant_keys: continue
                #if debug: print(key)

                if valid_obs:
                    if datetime not in obs[ID][location]:
                        obs[ID][location].append(datetime)

                    if clear_key in bufr_obs_time_keys:
                        try: value = ec.codes_get( bufr, key )
                        except Exception as e:
                            if verbose: print(FILE, key, e)
                            if traceback: gf.print_trace(e)
                            log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                            if verbose: print(log_str)
                            continue

                        # skip 1min ww and RR which are reported 10 times; 10min resolution is sufficient for us
                        if clear_key == "delayedDescriptorReplicationFactor":
                            if value == 10: skip_next = 10
                            continue

                        if value not in null_vals:

                            obs[ID][location].append(  (clear_key, value) )
                            
                            # avoid duplicate modifier keys (like timePeriod or depthBelowLandSurface) 
                            if clear_key in modifier_keys:
                                try:
                                    if clear_key == obs[ID][location][datetime][-2][0]:
                                        del obs[ID][location][datetime][-2]
                                except: pass
                            else: new_obs += 1

                else:
                    if not subset and key in typical_keys:
                        typical[key]    = ec.codes_get( bufr, key )
                        if typical[key] in null_vals: del typical[key]
                        last_key        = "typical"
                        continue
                    
                    if clear_key in station_keys:
                        #meta[clear_key] = ec.codes_get(bufr, key)
                        try: meta[clear_key] = ec.codes_get(bufr, key)
                        except: meta[clear_key] = ec.codes_get_array(bufr, key)[0]
                        #TODO some OGIMET-BUFRs seem to contain multiple station numbers in one key (arrays)
                        
                        if meta[clear_key] in null_vals:
                            del meta[clear_key]; continue
                    
                        # check for identifier of DWD stations (in German: "nebenamtliche Stationen")
                        if "shortStationName" in meta:
                            location = meta["shortStationName"]
                            station_type = "dwd"; skip_next = 4
                            
                        # check if all essential station information keys for a WMO station are present
                        elif { "stationNumber", "blockNumber" }.issubset( set(meta) ):
                            location = str(meta["stationNumber"] + meta["blockNumber"] * 1000).rjust(5,"0") + "0"
                            station_type = "wmo"
                            if source in {"test","DWD","COD","NOAA"}: skip_next = 2
                        
                        if location:
                            if location not in known_stations:
                                meta = {}; location = None; skip_obs = True
                                if source in {"test","DWD","COD","KNMI","RMI","NOAA"}:
                                    if station_type == "wmo":   skip_next = 11
                                    elif station_type == "dwd": skip_next = 13
                            else: obs[ID][location] = []

                    elif location:
                        
                        if clear_key in time_keys: # {year, month, day, hour, minute}
                            meta[clear_key] = ec.codes_get_long(bufr, key)
                            if meta[clear_key] in null_vals: del meta[clear_key]
                        
                            if clear_key == "minute":
                                # check if all essential time keys are now present
                                valid_obs = time_keys.issubset(meta)
                                if valid_obs:
                                    datetime = to_datetime(meta)
                                    if debug: print(meta)
                                    if source in {"test","DWD","COD","NOAA"}: skip_next = 4
                                    continue
                                
                                elif time_keys_hour.issubset(meta):
                                    # if only minute is missing, assume that minute == 0
                                    meta["minute"] = 0; valid_obs = True
                                    datetime = to_datetime(meta)
                                    if debug: print("minute0:", meta)
                                    continue
                                
                                # if we are still missing time keys: use the typical information
                                elif typical:
                                    # use the typical values we gathered earlier keys are missing
                                    for i,j in zip(sorted_time_keys, sorted_typical_keys):
                                        try:    meta[i] = int(typical[j])
                                        except: pass
                                    
                                    # again, if only minute is missing, assume that minute == 0
                                    if time_keys_hour.issubset(meta):
                                        meta["minute"] = 0; valid_obs = True;
                                        datetime = to_datetime(meta); continue

                                    # no luck? possibly, there could be typicalDate or typicalTime present
                                    if not {"year","month","day"}.issubset(set(meta)) and "typicalDate" in typical:
                                        typical_date    = typical["typicalDate"]
                                        meta["year"]    = int(typical_date[:4])
                                        meta["month"]   = int(typical_date[4:6])
                                        meta["day"]     = int(typical_date[-2:])
                                    else: skip_obs = True; continue

                                    if ("hour" not in meta or "minute" not in meta) and "typicalTime" in typical:
                                        typical_time    = typical["typicalTime"]
                                        if "hour" not in meta: meta["hour"] = int(typical_time[:2])
                                        if "minute" not in meta: meta["minute"] = int(typical_time[2:4])
                                    else: skip_obs = True; continue
                                
                                else: skip_obs = True


            # end of while loop
            ec.codes_keys_iterator_delete(iterid)

        # end of with clause (closes file handle)
        ec.codes_release(bufr)
        if new_obs: file_statuses.add( ("parsed", ID) );    log.debug(f"PARSED: '{FILE}'")
        else:       file_statuses.add( ("empty", ID) );     log.info(f"EMPTY:  '{FILE}'")
        
        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            
            db = database(db_file, timeout=timeout_db, traceback=traceback)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = convert_keys_se(obs, source, modifier_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=verbose)
            if obs_db:
                gf.obs_to_station_databases( obs_db, output_path, max_retries, timeout_station, verbose )
            
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()
    
    
    db = database(db_file, timeout=timeout_db, traceback=traceback)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
    db.close()

    obs_db = convert_keys_se(obs, source, modifier_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=verbose)
    #if use_shelve:
    #   for ID in obs: obs[ID].close()

    if obs_db: gf.obs_to_station_databases(obs_db, output_path, max_retries, timeout_station, verbose=verbose)
     
    # restore previous state of ECCODES_DEFINITION_PATH environment variable
    #if "tables" in config_bufr: os.environ['ECCODES_DEFINITION_PATH'] = old_path
    
    # remove file containing the pid, so the script can be started again
    if pid_file: os.remove( pid_file )
    

if __name__ == "__main__":
    
    msg    = "Decode one or more BUFR files and insert relevant observation data into station databases. "
    msg   += "NOTE: Setting a command line flag or option always overwrites the setting from the config file!"
    parser = argparse.ArgumentParser(description=msg)
 
    # add arguments to the parser
    log_levels = { "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG" }
    parser.add_argument("-l","--log_level", choices=log_levels, default="NOTSET", help="set log level")
    parser.add_argument("-i","--pid_file", action='store_true', help="create a pid file to check if script is running")
    parser.add_argument("-f","--file", help="parse single file bufr file, will be handled as source=extra by default")
    parser.add_argument("-v","--verbose", action='store_true', help="show detailed output")
    parser.add_argument("-p","--profiler", help="enable profiler of your choice (default: None)") #TODO -> prcs
    parser.add_argument("-c","--config", default="config.yaml", help="set name of yaml config file")
    parser.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    parser.add_argument("-d","--dev_mode", action='store_true', help="enable or disable dev mode")
    parser.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    parser.add_argument("-n","--max_files", type=int, help="maximum number of files to parse (per source)")
    #TODO sort_files replace by shelve
    parser.add_argument("-s","--sort_files", action='store_true', help="sort files alpha-numeric before parsing")
    parser.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    parser.add_argument("-b","--debug", action='store_true', help="enable or disable debugging")
    parser.add_argument("-e","--extra", default="extra", help="source name when parsing single file (default: extra)")
    parser.add_argument("-r","--redo", action='store_true', help="decode bufr again even if already processed")
    parser.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")

    args = parser.parse_args()

    #read yaml configuration file config.yaml into dictionary
    config          = gf.read_yaml( args.config )
    config_script   = config["scripts"][sys.argv[0]]
    conda_env = os.environ['CONDA_DEFAULT_ENV']
    
    if config_script["conda_env"] != conda_env:
        sys.exit(f"This script needs to run in conda environment {config_script['conda_env']}, exiting!")
 
    if args.max_files:  config_script["max_files"]  = args.max_files
    if args.sort_files: config_script["sort_files"] = args.sort_files

    if args.pid_file:               config_script["pid_file"] = True
    if config_script["pid_file"]:
        pid_file = sys.argv[0] + ".pid"
        if gf.already_running( pid_file ):
            sys.exit( f"{sys.argv[0]} is already running... exiting!" )
    else: pid_file = None

    if args.profiler:
        config_script["profiler"] = args.profiler
    if config_script["profiler"]:
        import importlib
        profiler    = importlib.import_module(config_script["profiler"])
        profile     = True
    else: profile = False
    
    if args.log_level: config_script["log_level"] = args.log_level
    log.basicConfig(filename=f"{sys.argv[0]}.log", filemode="w", level=eval(f"log.{config_script['log_level']}"))

    started_str = f"STARTED {sys.argv[0]} @ {dt.utcnow()}"; log.info(started_str)

    if args.verbose:    verbose = True
    else:               verbose = config_script["verbose"]
    if verbose: print(started_str)

    if args.debug:                  config_script["debug"] = True
    if config_script["debug"]:      import pdb; debug = True
    else:                           debug = False 

    if args.traceback:  traceback = True
    else:               traceback = config_script["traceback"]

    null_vals = { ec.CODES_MISSING_LONG, ec.CODES_MISSING_DOUBLE } # (2147483647, -1e+100)
    for i in config_script["null_vals"]: null_vals.add( i )

    station_keys        = frozenset(config_script["station_keys"])
    time_keys           = frozenset(config_script["time_keys"])
    time_keys_hour      = frozenset(config_script["time_keys"][:4])
    typical_time_keys   = frozenset({ "typical"+i.capitalize() for i in time_keys })
    typical_keys        = frozenset(typical_time_keys | {"typicalDate","typicalTime"})
    sorted_time_keys    = sorted(time_keys)
    sorted_typical_keys = sorted(typical_time_keys)

    if args.timeout:        timeout_station = args.timeout
    else:                   timeout_station = config_script["timeout"]
    
    if args.max_retries:    max_retries = args.max_retries
    else:                   max_retries = config_script["max_retries"]

    timeout_db  = config["database"]["timeout"]
    db_file     = config["database"]["db_file"]

    if args.dev_mode:               config_script["dev_mode"] = True
    if config_script["dev_mode"]:   output_path = config_script["output_dev"]
    else:                           output_path = config_script["output_oper"]

    output_path += "/raw"; gf.create_dir(output_path)

    # add files table (file_table) to main database if not exists
    #TODO this should be done during initial system setup, file_table should be added there
    db = database(db_file, timeout=timeout_db, traceback=traceback)
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    db.close()

    # parse the BUFR translation and bufr flags yaml files into dictionaries
    bufr_translation    = gf.read_yaml( config_script["bufr_translation"] )
    time_periods        = bufr_translation["timePeriod"]
    bufr_flags          = gf.read_yaml( config_script["bufr_flags"] )

    # get the keys from the conversion dictionary, skip the first 5 which are used for unit conversions
    bufr_translation_keys = list(bufr_translation)[5:]; bufr_translation_keys.remove("cloudBase")

    bufr_keys           = frozenset( bufr_translation_keys )
    bufr_obs_time_keys  = frozenset( bufr_keys | {"timePeriod"} )

    # get special types of keys
    modifier_keys, depth_keys, height_keys = set(), set(), set()

    for i in bufr_translation:
        if type(bufr_translation[i]) == dict:
            try:    subkey = list(bufr_translation[i])
            except: continue
            if type(subkey[0]) == float:
                if subkey[0] < 0:   depth_keys.add(i)
                elif subkey[0] > 0: height_keys.add(i)
        elif type(bufr_translation[i]) == type(None): modifier_keys.add(i)
   
    modifier_keys.add("timePeriod")

    # union of both will be used later
    height_keys, depth_keys = frozenset(height_keys), frozenset(depth_keys)
    height_depth_keys       = frozenset( height_keys | depth_keys )

    relevant_keys = bufr_obs_time_keys | station_keys | typical_keys | time_keys

    #parse command line arguments
    if args.source:
        source = config["sources"][args.source]

        if "," in source:
            sources = source.split(","); config_sources = {}
            for s in sources:
                config_sources[s] = config["sources"][s]

        else: config_sources = { args.source : config["sources"][args.source] }
   
    elif args.file: parse_all_BUFRs( file=args.file, pid_file=pid_file )
    else:           config_sources = config["sources"]
    
    if not args.file:
        for SOURCE in config_sources:
            if verbose: print(f"Parsing source {SOURCE}...")
            parse_all_BUFRs( source = SOURCE, pid_file=pid_file )

    finished_str = f"FINISHED {sys.argv[0]} @ {dt.utcnow()}"; log.info(finished_str)
    if verbose: print(finished_str)
