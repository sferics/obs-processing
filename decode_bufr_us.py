#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import argparse, sys, os, psutil#, shelve
import numpy as np
from glob import glob 
import eccodes as ec        # bufr decoder by ECMWF
from datetime import datetime as dt, timedelta as td
import global_functions as gf
import global_variables as gv
from database import database_class
from bufr import bufr_class
from obs import obs_class

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible
#TODO raises error "API not implemented in CFFI porting"
#see: https://github.com/ecmwf/eccodes-python#experimental-features
#ec.codes_no_fail_on_wrong_length(True)


def decode_bufr_us( source=None, file=None, known_stations=None, pid_file=None ):
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
    start, end, subset_extract, subset_interval, subset_list = (None for _ in range(5))
    if args.extract_subsets:
        try:    subset_extract = int(args.extract_subsets) - 1
        except:
            if "-" in args.extract_subsets:
                subset_interval = args.extract_subsets.split("-")
                start, end = subset_interval[0], subset_interval[1]
                try:                start = int(start)
                except ValueError:  assert( start == "" )
                try:                end = int(end)
                except ValueError:  assert( end == "" )
                # start of interval can not be larger than its end
                if type(start) == type(end) == int: assert( 0 <= start <= end )
            elif "," in args.extract_subsets:
                subset_list = args.extract_subsets.split(",")
                for i in range(len(subset_list)): subset_list[i] -= 1
                subset_list = set(subset_list)

    if source:
        config_source   = config_sources[source]
        if "bufr" in config_source:
             config_bufr = [config["bufr"], config_script, config_source["general"], config_source["bufr"]]
        else: return

        # previous dict entries will get overwritten by next list item during merge (right before left)
        config_bf = gf.merge_list_of_dicts( config_bufr )

        bf = bufr_class(config_bf, script=script_name[-5:-3])

        bufr_dir = bf.dir + "/"

        if hasattr(bf, "filter"):
            filter_keys             = set(bf.filter)
            number_of_filter_keys   = len(filter_keys)

            fun = lambda x : not pd.isnull(x);      filters     = {}
            for i in filter_keys:                   filters[i]  = fun
        else: filters = {}; number_of_filter_keys = float("inf")

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
                #codes_set(msgid, 'skipExtraKeyAttributes', 1)
                ec.codes_set(bufr, "unpack", 1)
                if args.extract_subsets:
                    subsets = ec.codes_get_long( bufr, "numberOfSubsets" )
                    if subsets > 1:
                        if subset_extract is not None:
                            ec.codes_set(bufr, "extractSubset", subset_extract)
                        elif subset_interval is not None:
                            if not start:  start  = 1
                            if not end:    end    = subsets
                            ec.codes_set(bufr, "extractSubsetIntervalStart", start-1);
                            ec.codes_set(bufr, "extractSubsetIntervalEnd", end-1)
                        elif subset_list is not None:
                            ec.codes_set(bufr, "extractSubsetList", subset_list)
                ec.codes_set(bufr, "doExtractSubsets", 1)
            except Exception as e:
                log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                if verbose: print(log_str)                
                if traceback: gf.print_trace(e)
                file_statuses.add( ("error", ID) )
                continue
            else: obs_bufr[ID] = {} #shelve.open(f"shelves/{ID}", writeback=True)
            
            iterid = ec.codes_bufr_keys_iterator_new(bufr)
            
            if config_script["skip_computed"]:      ec.codes_skip_computed(iterid)
            if config_script["skip_function"]:      ec.codes_skip_function(iterid)
            if config_script["skip_duplicates"]:    ec.codes_skip_duplicates(iterid)

            meta, typical   = {}, {}
            valid_obs       = False
            location        = None
            skip_next       = 10
            subset, new_obs = 0, 0
            skip_obs        = False
            last_key        = None

            if debug: pdb.set_trace()

            while ec.codes_bufr_keys_iterator_next(iterid):
                
                if skip_next: skip_next -= 1; continue

                key = ec.codes_bufr_keys_iterator_get_name(iterid)
                
                if last_key == "typical" and last_key not in key:
                    last_key = None; skip_next = 3; continue

                if key == "subsetNumber":
                    if subset > 0:
                        meta = {}; location = None; valid_obs = False; skip_obs = False
                    subset += 1; continue
                elif skip_obs: continue

                clear_key = bf.clear(key)
                if clear_key not in bf.relevant_keys: continue
                #if debug: print(key)

                if valid_obs:
                    if location not in obs_bufr[ID]:
                        obs_bufr[ID][location] = {}

                    if clear_key in bf.obs_time_keys:
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

                        if value not in bf.null_vals:
                            
                            # get the BUFR code number which gives us all necessary unit and scale info
                            code        = ec.codes_get_long( bufr, key + "->code" )
                            obs_data    = ( code, value )
                            
                            #TODO use a defaultdict instead
                            #TODO find out which is faster (should be try/except in this case)
                            """
                            if datetime not in stations[location]:
                                stations[location][datetime] = []
                            stations[location][datetime].append( obs_data )
                            """
                            try:    obs_bufr[ID][location][datetime].append(obs_data)
                            except: obs_bufr[ID][location][datetime] = [obs_data]
                            # avoid duplicate modifier keys (like timePeriod or depthBelowLandSurface) 
                            if clear_key in bf.modifier_keys:
                                try:
                                    if clear_key == obs_bufr[ID][location][datetime][-2][0]:
                                        del obs_bufr[ID][location][datetime][-2]
                                except: pass
                            new_obs += 1

                else:
                    if not subset and key in bf.typical_keys:
                        typical[key]    = ec.codes_get( bufr, key )
                        if typical[key] in bf.null_vals: del typical[key]
                        last_key        = "typical"
                        continue
                    
                    if location is None and clear_key in bf.station_keys:
                        #meta[clear_key] = ec.codes_get(bufr, key)
                        try: meta[clear_key] = ec.codes_get(bufr, key)
                        except: meta[clear_key] = ec.codes_get_array(bufr, key)[0]
                        #TODO some OGIMET-BUFRs seem to contain multiple station numbers in one key (arrays)
                        
                        if meta[clear_key] in bf.null_vals:
                            del meta[clear_key]; continue
                    
                        # check for identifier of DWD stations (in German: "nebenamtliche Stationen")
                        if "shortStationName" in meta:
                            location = meta["shortStationName"]
                            station_type = "dwd"; skip_next = 4
                            
                        # check if all essential station information keys for a WMO station are present
                        elif { "stationNumber", "blockNumber" }.issubset( set(meta) ):
                            location = str(meta["stationNumber"] + meta["blockNumber"]*1000).rjust(5,"0") + "0"
                            station_type = "wmo"
                            if "skip1" in config_bufr: skip_next = config_bufr["skip1"]
                            elif source in {"DWD","COD","NOAA"}: skip_next = 2
                        
                        if location and location not in known_stations:
                            meta = {}; location = None; skip_obs = True
                            if station_type == "dwd": skip_next = 13
                            elif "skip2" in config_bufr: skip_next = config_bufr["skip2"]
                            elif source in {"DWD","COD","KNMI","RMI","NOAA"}:
                                if station_type == "wmo":   skip_next = 11
 
                    elif location:
                        
                        if clear_key in bf.set_time_keys: # {year, month, day, hour, minute}
                            meta[clear_key] = ec.codes_get_long(bufr, key)
                            if meta[clear_key] in bf.null_vals: del meta[clear_key]
                        
                            if clear_key == "minute":
                                # check if all essential time keys are now present
                                valid_obs = bf.set_time_keys.issubset(meta)
                                if valid_obs:
                                    datetime = gf.to_datetime(meta)
                                    if debug: print(meta)
                                    if "skip3" in config_bufr: skip_next = config_bufr["skip3"]
                                    elif source in {"DWD","COD","NOAA"}: skip_next = 4
                                    continue
                                
                                elif bf.time_keys_hour.issubset(meta):
                                    # if only minute is missing, assume that minute == 0
                                    meta["minute"] = 0; valid_obs = True
                                    datetime = gf.to_datetime(meta)
                                    if debug: print("minute0:", meta)
                                    continue
                                
                                # if we are still missing time keys: use the typical information
                                elif typical:
                                    # use the typical values we gathered earlier keys are missing
                                    for i,j in zip(bf.time_keys, bf.typical_keys):
                                        try:    meta[i] = int(typical[j])
                                        except: pass
                                    
                                    # again, if only minute is missing, assume that minute == 0
                                    if bf.time_keys_hour.issubset(meta):
                                        meta["minute"] = 0; valid_obs = True; continue

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

        if new_obs:
            file_statuses.add( ("parsed", ID) )
            log.debug(f"PARSED: '{FILE}'")
        else:
            file_statuses.add( ("empty", ID) )
            log.info(f"EMPTY:  '{FILE}'")
        
        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            
            db = database_class(config=config_database)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = bf.convert_keys_us( obs_bufr, source )
            if obs_db: obs.to_station_databases( obs_db )
            
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()
    
    db = database_class(config=config_database)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=bf.timeout)
    db.close()

    if verbose: print( obs_bufr )

    obs_db = bf.convert_keys_us( obs_bufr, source )
    #for ID in obs: obs_bufr[ID].close()

    if obs_db: obs.to_station_databases( obs_db )
     
    # restore previous state of ECCODES_DEFINITION_PATH environment variable
    if "tables" in config_bufr: old_path = os.environ['ECCODES_DEFINITION_PATH']
    
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
    parser.add_argument("-p","--profiler", help="enable profiler of your choice (default: None)")
    #TODO replace profiler by number of processes (prcs) when real multiprocessing (using module) is implemented
    parser.add_argument("-c","--clusters", help="station clusters to consider, comma seperated")
    parser.add_argument("-C","--config", default="config", help="set name of config file")
    parser.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    parser.add_argument("-d","--dev_mode", action='store_true', help="enable or disable dev mode")
    parser.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    parser.add_argument("-n","--max_files", type=int, help="maximum number of files to parse (per source)")
    parser.add_argument("-s","--sort_files", action='store_true', help="sort files alpha-numeric before parsing")
    parser.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    parser.add_argument("-b","--debug", action='store_true', help="enable or disable debugging")
    parser.add_argument("-x","--extract_subsets", help="extract specific subsets only ('N', 'x,y,z' or 'start-end')")
    parser.add_argument("-k","--skip", default="", help="skip [c]omputed, [f]unction and/or [d]uplicate keys")
    parser.add_argument("-e","--extra", default="extra", help="source name when parsing single file (default: extra)")
    parser.add_argument("-r","--redo", action='store_true', help="decode bufr again even if already processed")
    parser.add_argument("-R","--restart", help=r"only parse all files with status 'locked_{pid}'")
    parser.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")

    #TODO add shelve option to save some RAM

    args = parser.parse_args()

    #read configuration file into dictionary
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

    #if args.dev_mode:               config_script["dev_mode"] = True
    #if config_script["dev_mode"]:   output_path = config_script["output_dev"]
    #else:                           output_path = config_script["output_oper"]

    output_path = config_script["output_path"]

    if args.clusters: config_source["clusters"] = set(args.clusters.split(","))

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

    elif args.file: decode_bufr_us( file=args.file, pid_file=pid_file )
    else:           config_sources = config["sources"]

    if not args.file:
        for SOURCE in config_sources:
            if verbose: print(f"Parsing source {SOURCE}...")
            decode_bufr_us( source = SOURCE, pid_file=pid_file )

    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)
    if verbose:
        print(finished_str)
        time_taken = stop_time - start_time
        print(f"{time_taken.seconds}.{time_taken.microseconds} s")
