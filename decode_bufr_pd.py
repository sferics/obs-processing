#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import argparse, sys, os, psutil, pdbufr
import numpy as np
from glob import glob 
import eccodes as ec        # bufr decoder by ECMWF
import pandas as pd
from collections import defaultdict
from datetime import datetime as dt, timedelta as td
from database import database_class
from bufr import bufr_class
from obs import obs_class
import global_functions as gf
import global_variables as gv
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", module="pdbufr")

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible

def decode_bufr_pd( source=None, file=None, known_stations=None, pid_file=None ):
    #TODO
    """
    Parameter:
    ----------
    source : name of source (str)
    pid_file : name of the file where the process id gets stored (str)

    Notes:
    ------
    main function of the script, parses all files of a given source and tries to save them in database
    using the obs.to_station_databases() function (from the obs class). includes file handling and sets the status of a file to
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
             config_list = [config["bufr"], config_script, config_source["general"], config_source["bufr"]]
        else: return
        
        # previous dict entries will get overwritten by next list item during merge (right before left)
        config_bufr = gf.merge_list_of_dicts( config_list )

        bf = bufr_class(config_bufr, script=script_name[-5:-3])

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
        
        config_bufr = gf.merge_list_of_dicts( [config["bufr"], config_script] )
        bf          = bufr_class(config_bufr, script=script_name[-5:-3])

    #TODO use defaultdic instead
    obs_bufr, file_statuses = {}, set()
    new_obs = 0

    # initialize obs class (used for saving obs into station databases)
    # in this merge we are adding only already present keys; while again overwriting them
    config_obs  = gf.merge_list_of_dicts([config["obs"], config_script], add_keys=False)
    obs         = obs_class( config_obs, source, mode=config_script["mode"] )

    for FILE in files_to_parse:
        
        ID = file_IDs[FILE]
        obs_bufr[ID] = {}
        
        PATH = bufr_dir + FILE
        if verbose: print(PATH)

        #TODO from here on we could outsource into another function and would probably just need one decode_bufr.py
        
        #TODO for some reason my nice NaN removal filter doesnt work; fix or let it be...
        #df = pdbufr.read_bufr(PATH, filters=filters, columns="all", flat=True, required_columns=required_keys)
        #df = pdbufr.read_bufr(PATH, columns="all", flat=True, required_columns=required_keys)
        #df = pdbufr.read_bufr(PATH, flat=True, columns="data")
        #print(df)
        df = pdbufr.read_bufr(PATH, columns=bf.relevant_keys, required_columns=bf.required_keys)

        # len(df.index) == 0 is much faster than df.empty or len(df) == 0
        # https://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty
        # if the dataframe contains no data or no stations with WMO IDs, skip
        if len(df.index) == 0 or df.loc[:, bf.wmo].isna().all():
            file_statuses.add( ("empty", ID) ); continue

        # if dataframe larger than minimum keyset: drop all rows and columns which only contains NaNs
        #elif len(df.columns) > number_of_filter_keys:
        else:
            df.dropna(how="all", inplace=True)
            #print(df.shape)
            if len(df.index) == 0: file_statuses.add( ("empty", ID) ); continue
            #df.dropna(how="all", axis=1, inplace=True)
            #print(df.shape)

        #TODO use typical datetime if no datetime present
        #typical_datetime = "typical_datetime"
        time_period = ""
        cor         = 0

        for ix, row in df.iterrows():

            #keys_not_na = bufr_obs_keys.intersection(row.index)
            # in future versions of pandas we will need this next line:
            #keys_not_na = list(bufr_obs_keys.intersection(row.index))

            #if row.loc[keys_not_na].isna().all(): continue

            #keys_not_na = bufr_obs_keys.intersection(row.index)
            #if not row.loc[bufr_obs_keys].notna().any(): continue
 
            #TODO possibly BUG in pdbufr? timePeriod=0 never exists; write bug report in github!
            try:
                if pd.notna(row[bf.tp]): time_period = row[bf.tp]
            except: pass 

            try:
                if time_period == -1 and row[bf.replication] == 10 and (pd.notna(row[bf.ww]) or pd.notna(row[bf.rr])):
                    continue
            except: pass
            
            location = str(row[bf.wmo]) + "0"
            if location not in known_stations: continue
            
            datetime = row[bf.dt]
            if pd.isna(datetime):
                if verbose: print("NO DATETIME:", FILE)
                continue
            
            for i in (bf.replication, bf.tp, bf.wmo, bf.dt):
                try:    del row[i]
                except: continue
                
            if location not in obs_bufr[ID]:            obs_bufr[ID][location]           = {}
            if datetime not in obs_bufr[ID][location]:  obs_bufr[ID][location][datetime] = {}

            modifier_list = []
            for key in (bf.obs_sequence, bf.sensor_height, bf.sensor_depth, bf.vertical_signf):
                try:    
                    if pd.notna(row[key]): modifier_list.append((key, row[key]))
                except: continue
                else:   del row[key]

            obs_list = []
            for key, val in zip(row.index, row):
                if pd.notna(val): obs_list.append((key, val))

            if modifier_list and obs_list: obs_list = modifier_list + obs_list
            if obs_list:
                try:    obs_bufr[ID][location][datetime][time_period] += obs_list
                except: obs_bufr[ID][location][datetime][time_period] = obs_list
                new_obs += 1

        if new_obs:
            file_statuses.add( ("parsed", ID) )
            log.debug(f"PARSED: '{FILE}'")
        else:
            file_statuses.add( ("empty", ID) )
            log.info(f"EMPTY:  '{FILE}'")


        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= bf.min_ram:
            
            db = database_class(config=config_database)
            db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = bf.convert_keys_pd( obs_bufr, source )
            if obs_db: obs.to_station_databases(obs_db)
            
            if pid_file: os.remove( pid_file )

            # https://stackoverflow.com/a/33334183
            # cleanup file objects and descriptors before restart
            try:
                p = psutil.Process(os.getpid())
                for handler in p.open_files() + p.connections():
                    os.close(handler.fd)
            except Exception as e:
                log.error(e)

            exe = sys.executable # restart program with same arguments and add restart flag
            os.execl(exe, exe, * sys.argv, "-R", pid); sys.exit()


    db = database_class(config=config_database)
    db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
    db.close(commit=True)
    
    if verbose: print(obs_bufr)
    obs_db = bf.convert_keys_pd( obs_bufr, source )

    if verbose: print(obs_db)
    if obs_db: obs.to_station_databases(obs_db)
     
    # remove file containing the pid, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":
    
    msg    = "Decode one or more BUFR files and insert relevant observation data into station databases. "
    msg   += "NOTE: Setting a command line flag or option always overwrites the setting from the config file!"
    parser = argparse.ArgumentParser(description=msg)
 
    # add arguments to the parser
    parser.add_argument("-l","--log_level", choices=gv.log_levels, default="NOTSET", help="set log level")
    parser.add_argument("-i","--pid_file", action='store_true', help="create a pid file to check if script is running")
    parser.add_argument("-f","--file", help="parse single file bufr file, will be handled as source=extra by default")
    parser.add_argument("-v","--verbose", action='store_true', help="show detailed output")
    parser.add_argument("-p","--profiler", help="enable profiler of your choice (default: None)") #TODO -> prcs
    parser.add_argument("-c","--clusters", help="station clusters to consider, comma seperated")
    parser.add_argument("-C","--config", default="config", help="set name of config file")
    parser.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    parser.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    parser.add_argument("-M","--mode", default=None, help="set mode of operation (default: None)")
    parser.add_argument("-n","--max_files", type=int, help="maximum number of files to parse (per source)")
    parser.add_argument("-s","--sort_files", action='store_true', help="sort files alpha-numeric before parsing")
    parser.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    parser.add_argument("-d","--debug", action='store_true', help="enable or disable debugging")
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

    if args.traceback:              config_script["traceback"] = True

    if args.timeout:                config_script["timeout"] = args.timeout
    
    if args.max_retries:            config_script["max_retries"] = max_retries = args.max_retries
    else:                           max_retries = config_script["max_retries"]

    if args.mode:                   config_script["mode"] = args.mode

    if args.clusters: config_source["clusters"] = set(args.clusters.split(",")) 

    # get configuration for the initialization of the database class
    config_database = config["database"]

    # add files table (file_table) to main database if not exists
    #TODO this should be done during initial system setup, file_table should be added there
    db = database_class(config=config_database)
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    db.close()

    # parse command line arguments
    if args.file: decode_bufr_pd( file=args.file, pid_file=pid_file ) # source=args.source
    elif args.source:
        source = config["sources"][args.source]

        if "," in source:
            sources = source.split(","); config_sources = {}
            for s in sources:
                config_sources[s] = config["sources"][s]

        else: config_sources = { args.source : config["sources"][args.source] }
   
    else:           config_sources = config["sources"]
    
    if not args.file:
        for SOURCE in config_sources:
            if verbose: print(f"Parsing source {SOURCE}...")
            decode_bufr_pd( source = SOURCE, pid_file=pid_file )

    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)
    if verbose:
        print(finished_str)
        time_taken = stop_time - start_time
        print(f"{time_taken.seconds}.{time_taken.microseconds} s")
