#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import sys, os
import argparse, sqlite3, psutil
from copy import copy
import random, time
import numpy as np
from glob import glob 
import eccodes as ec        # bufr decoder by ECMWF
from pathlib import Path    # path operation
from datetime import datetime as dt, timedelta as td, timezone as tz
import global_functions as gf
import global_variables as gv
from database import database_class
from bufr import bufr_class
from obs import obs_class

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible

#TODO set local tables
#ec.codes_set_definitions_path

def decode_bufr_se( source=None, file=None, known_stations=None, pid_file=None ):
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
        if "bufr" in config_source:
            config_bufr = [config["bufr"], config_script, config_source["general"], config_source["bufr"]]
        else: return

        # previous dict entries will get overwritten by next list item during merge (right before left)
        config_bf = gf.merge_list_of_dicts( config_bufr )

        bf = bufr_class(config_bf, script=script_name[-5:-3])
        bufr_dir = bf.dir + "/"
        
        try:    clusters = set(config_source["clusters"].split(","))
        except: clusters = None

        db = database_class(config=config_database)

        for i in range(max_retries):
            try:    known_stations = db.get_stations( clusters )
            except: pass
            else:   break

        if i == max_retries - 1: sys.exit(f"Can't access main database, tried {max_retries} times. Is it locked?")

        if hasattr(bf, "glob") and bf.glob: ext = f"{bf.glob}.{bf.ext}"
        else:                               ext = f"*.{bf.ext}" #TODO add possibility to use multiple extensions (set)

        if args.restart:
            files_to_parse = set(db.get_files_with_status( f"locked_{args.restart}", source ))
        else:
            files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + ext )))

            if args.redo:   skip_files  = db.get_files_with_status( r"locked_%", source )
            else:           skip_files  = db.get_files_with_status( bf.skip_status, source )

            files_to_parse = list( files_in_dir - skip_files )

            #TODO special sort functions for CCA, RRA and stuff in case we dont have sequence key
            #TODO implement order by datetime of files
            if bf.sort_files: files_to_parse = sorted(files_to_parse)
            if bf.max_files:  files_to_parse = files_to_parse[:bf.max_files]

            if verbose:
                print("#FILES in DIR:  ",   len(files_in_dir))
                print("#FILES to skip: ",   len(skip_files))

        if verbose: print("#FILES to parse:",   len(files_to_parse))

        gf.create_dir( bf.dir )

        file_IDs = {}

        for FILE in files_to_parse:

            file_path = gf.get_file_path( bufr_dir + FILE )
            file_date = gf.get_file_date( file_path )

            ID = db.get_file_id(FILE, file_path)
            if not ID:
                status = f"locked_{pid}"
                ID = db.register_file(FILE, file_path, source, status, file_date, verbose=verbose)

            file_IDs[FILE] = ID

        db.close(commit=True)

        #TODO if multiprocessing: split file_to_parse by number of processes (eg 8) and parse files simultaneously
        #see https://superfastpython.com/restart-a-process-in-python/

    elif file:

        FILE            = file.split("/")[-1]
        files_to_parse  = (FILE,)
        file_path       = gf.get_file_path(args.file)
        file_date       = gf.get_file_date(args.file)
        bufr_dir        = "/".join(file.split("/")[:-1]) + "/"
        source          = args.extra # default: extra

        db = database_class(config=config_database)
        known_stations  = db.get_stations()

        ID = db.get_file_id(FILE, file_path)
        if ID:  db.set_file_status(ID,"locked")
        else:   ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)

        db.close(commit=True)

        file_IDs = {FILE:ID}

        config_bf   = gf.merge_list_of_dicts( [config["bufr"], config_script] )
        bf          = bufr_class(config_bf, script=script_name[-5:-3])

    #TODO use defaultdic instead
    obs_bufr, file_statuses = {}, set()
    new_obs = 0

    # initialize obs class (used for saving obs into station databases)
    # in this merge we are adding only already present keys; while again overwriting them
    config_obs  = gf.merge_list_of_dicts([config["obs"], config_script], add_keys=False)
    obs         = obs_class("raw", config_obs, source)

    for FILE in files_to_parse:

        ID = file_IDs[FILE]
        PATH = bufr_dir + FILE

        if debug: print(PATH)

        with open(PATH, "rb") as f:
            try:
                ID = file_IDs[FILE]
                # if for whatever reason no ID (database lock?) or filestatus means skip: continue with next file
                if not ID: continue
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    file_statuses.add( ("empty", ID) )
                    if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                ec.codes_set(bufr, "skipExtraKeyAttributes", 1)
                ec.codes_set(bufr, "unpack", 1)
            except Exception as e:
                log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                if verbose: print(log_str)
                if traceback: gf.print_trace(e)
                file_statuses.add( ("error", ID) )
                continue
            else:
                product_kind = ec.CODES_PRODUCT_BUFR
                if debug: print(product_kind)
                # if no SYNOP or automatic weather station, skip!
                if product_kind not in {0,1,2,6,7}: continue
                obs_bufr[ID] = {} #shelve.open(f"shelves/{ID}", writeback=True) #{}

            iterid = ec.codes_bufr_keys_iterator_new(bufr)
            #if debug: print(ec.codes_get_message(bufr))

            #TODO does this actually have an influence??? ec.codes_skip_duplicates
            for fun in (ec.codes_skip_computed, ec.codes_skip_function, ec.codes_skip_duplicates): fun(iterid)

            meta, typical   = {}, {}
            valid_obs       = False
            location        = None
            skip_next       = 10
            subset, new_obs = 0, 0
            skip_obs        = False
            last_key        = None
            prev_location   = None

            if debug: pdb.set_trace()
            subsets = (ec.codes_get_long(bufr, "numberOfSubsets") > 1)
            
            while ec.codes_bufr_keys_iterator_next(iterid):
                
                key = ec.codes_bufr_keys_iterator_get_name(iterid)

                if "observationSequenceNumber" in key:
                    print(key, ec.codes_get(bufr,key))

                if skip_next: skip_next -= 1; continue
                
                key = ec.codes_bufr_keys_iterator_get_name(iterid)

                if last_key == "typical" and last_key not in key:
                    last_key = None; skip_next = 3; continue
                
                if subsets: # if the message does contain more than 1 subsets, else skip this
                    if key == "subsetNumber":
                        if subset > 0:
                            meta = {}; location = None
                            valid_obs = False; skip_obs = False
                        subset += 1; continue
                    elif skip_obs: continue
                
                clear_key = bf.clear(key)
                if clear_key not in bf.relevant_keys: continue
                
                if valid_obs:
                    #if datetime not in obs_bufr[ID][location]: obs[ID][location][datetime] = []
                    
                    if clear_key in bf.bufr_mod_keys:# and not ec.codes_is_missing(bufr, key):

                        try: value = ec.codes_get( bufr, key )
                        except Exception as e:
                            if debug: print(FILE, key, e)
                            if traceback: gf.print_trace(e)
                            log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                            if verbose: print(log_str)
                            continue
                        
                        if value in bf.null_vals: continue
                        # skip 1min ww and RR which are reported 10 times; 10min resolution is sufficient for us
                        if clear_key == "delayedDescriptorReplicationFactor":
                            if value == 10:
                                skip_next = 10
                                if debug: print(obs_bufr[ID][location][datetime])
                                try:    del obs_bufr[ID][location][datetime][-1]
                                except: pass
                            continue

                        #obs_bufr[ID][location][datetime].append( (clear_key, value) )
                        try:    obs_bufr[ID][location][datetime].append( (clear_key, value) )
                        except: obs_bufr[ID][location][datetime] = []

                        # avoid duplicate modifier keys (like timePeriod or depthBelowLandSurface) 
                        if clear_key in bf.modifier_keys and value != -10:
                            try:
                                if clear_key == obs_bufr[ID][location][datetime][-2][0]:
                                    del obs_bufr[ID][location][datetime][-2]
                            except: pass
                        else: new_obs += 1

                else:
                    if not subset and key in bf.typical_keys:
                        typical[key] = ec.codes_get( bufr, key )
                        if typical[key] in bf.null_vals: del typical[key]
                        last_key = "typical"

                    elif clear_key in bf.station_keys:
                        meta[clear_key] = ec.codes_get(bufr, key)
                        #try:    meta[clear_key] = ec.codes_get(bufr, key)
                        #except: meta[clear_key] = ec.codes_get_array(bufr, key)[0]
                        #TODO some OGIMET-BUFRs seem to contain multiple station numbers in one key (arrays)
                        #if ec.codes_get_size(bufr, key) > 1:

                        if meta[clear_key] in bf.meta_null_vals:
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
                            if debug: print(location)
                            if location not in known_stations:
                                meta = {}; skip_obs = True; location = None
                                if source in {"test","DWD","COD","KNMI","RMI","NOAA"}:
                                    if station_type == "wmo":   skip_next = 11
                                    elif station_type == "dwd": skip_next = 13
                            else:
                                prev_location = copy(location)
                                obs_bufr[ID][location] = {}
                        elif subset > 0 and prev_location is not None: location = copy(prev_location)

                    elif location:

                        if clear_key in bf.time_keys:
                            meta[clear_key] = ec.codes_get_long(bufr, key)
                            if meta[clear_key] in bf.null_vals: del meta[clear_key]

                            if clear_key == "minute":
                                # check if all essential time keys are now present
                                valid_obs = bf.time_keys_set.issubset(meta)
                                if valid_obs:
                                    datetime = bf.to_datetime(meta)
                                    if debug: print(meta)
                                    if source in {"test","DWD","COD","NOAA"}: skip_next = 4

                                elif bf.time_keys_hour.issubset(meta):
                                    # if only minute is missing, assume that minute == 0
                                    meta["minute"] = 0; valid_obs = True
                                    datetime = to_datetime(meta)
                                    if debug: print("minute0:", meta)

                                # if we are still missing time keys: use the typical information
                                elif typical:
                                    # use the typical values we gathered earlier if keys are missing
                                    for i,j in zip(bf.time_keys, bf.typical_keys):
                                        try:    meta[i] = int(typical[j])
                                        except: pass

                                    # again, if only minute is missing, assume that minute == 0
                                    if bf.time_keys_hour.issubset(meta):
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

            # end of while loop: delete keys iterator
            ec.codes_keys_iterator_delete(iterid)

        # end of with clause (closes file handle)
        ec.codes_release(bufr)
        if new_obs: file_statuses.add( ("parsed", ID) );    log.debug(f"PARSED: '{FILE}'")
        else:       file_statuses.add( ("empty", ID) );     log.info(f"EMPTY:  '{FILE}'")

        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        #TODO idea for true multiprocessing: if memory is full. tell all processes to stop after current file
        # then, call convert_obs() and join all dict together before inserting to databases
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            """
            db = database_class(db_file, timeout=timeout_db, traceback=traceback)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
            db.close()
            """
            print("Too much RAM used, RESTARTING...")
            
            obs_db = bf.convert_keys_se(obs_bufr, source, verbose=verbose)
            #if obs_db: obs.to_station_databases( obs_db, bf.output_path, "raw", max_retries, timeout_station, verbose )
            
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()
    """
    db = database_class(db_file, timeout=timeout_db, traceback=traceback)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
    db.close()
    """ 
    obs_db = bf.convert_keys_se(obs_bufr, source, verbose=verbose)
    #if use_shelve:
    #   for ID in obs: obs_bufr[ID].close()

    #TODO if multiprocessing, join all obs_db dictionaries together before inserting into databases

    #if obs_db: obs.to_station_databases(obs_db, bf.output_path, "raw", max_retries, timeout_station, verbose=verbose)
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
    parser.add_argument("-c","--clusters", help="station clusters to consider, comma seperated")
    parser.add_argument("-C","--config", default="config.yaml", help="set name of yaml config file")
    parser.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    parser.add_argument("-d","--dev_mode", action='store_true', help="enable or disable dev mode")
    parser.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    parser.add_argument("-n","--max_files", type=int, help="maximum number of files to parse (per source)")
    #TODO replace sort_files by shelve -> move sorting to bufr class and custom sorting (if needed) to source config
    parser.add_argument("-s","--sort_files", action='store_true', help="sort files alpha-numeric before parsing")
    parser.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    parser.add_argument("-O","--output_path", help="output path where you want to create the station databases")
    parser.add_argument("-b","--debug", action='store_true', help="enable or disable debugging")
    parser.add_argument("-e","--extra", default="extra", help="source name when parsing single file (default: extra)")
    parser.add_argument("-r","--redo", action='store_true', help="decode bufr again even if already processed")
    parser.add_argument("-R","--restart", help=r"only parse all files with status 'locked_{pid}'")
    parser.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")

    args = parser.parse_args()

    #read yaml configuration file config.yaml into dictionary
    config          = gf.read_yaml( args.config )
    script_name     = gf.get_script_name(__file__)
    config_script   = config["scripts"][script_name]
    conda_env       = os.environ['CONDA_DEFAULT_ENV']

    if config_script["conda_env"] != conda_env:
        sys.exit(f"This script needs to run in conda environment {config_script['conda_env']}, exiting!")

    pid = str(os.getpid())

    if args.max_files is not None:  config_script["max_files"]  = args.max_files
    if args.sort_files: config_script["sort_files"] = args.sort_files

    if args.pid_file: config_script["pid_file"] = True
    if config_script["pid_file"]:
        pid_file = script_name + ".pid"
        if gf.already_running( pid_file ):
            sys.exit( f"{script_name} is already running... exiting!" )
    else: pid_file = None

    if args.profiler:
        config_script["profiler"] = args.profiler
    if config_script["profiler"]:
        import importlib
        profiler    = importlib.import_module(config_script["profiler"])
        profile     = True
    else: profile = False

    if args.log_level: config_script["log_level"] = args.log_level
    log = gf.get_logger(script_name)

    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"; log.info(started_str)

    if args.verbose is not None: config_script["verbose"] = args.verbose
    verbose = config_script["verbose"]
    if verbose: print(started_str)

    if args.debug:                  config_script["debug"] = True
    if config_script["debug"]:      import pdb; debug = True
    else:                           debug = False

    if args.traceback:              config_script["traceback"] = traceback = True
    else:                           traceback = config_script["traceback"]

    if args.timeout:                config_script["timeout"] = timeout_station = args.timeout
    else:                           timeout_station = config_script["timeout"]

    if args.max_retries:            config_script["max_retries"] = max_retries = args.max_retries
    else:                           max_retries = config_script["max_retries"]

    if args.output_path:            config_script["output_path"] = args.output_path

    if args.clusters:               config_source["clusters"] = set(args.clusters.split(","))

    # get configuration for the initialization of the database class
    config_database = config["database"]

    # add files table (file_table) to main database if not exists
    #TODO this should be done during initial system setup, file_table should be added there
    db = database_class(config=config_database)
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    db.close()

    #parse command line arguments
    if args.source:
        source = config["sources"][args.source]

        if "," in source:
            sources = source.split(","); config_sources = {}
            for s in sources:
                config_sources[s] = config["sources"][s]

        else: config_sources = { args.source : config["sources"][args.source] }

    elif args.file: decode_bufr_se( file=args.file, pid_file=pid_file )
    else:           config_sources = config["sources"]

    if not args.file:
        for SOURCE in config_sources:
            if verbose: print(f"Parsing source {SOURCE}...")
            decode_bufr_se( source = SOURCE, pid_file=pid_file )
    
    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)
    if verbose:
        print(finished_str)
        time_taken = stop_time - start_time
        print(f"{time_taken.seconds}.{time_taken.microseconds} s")
