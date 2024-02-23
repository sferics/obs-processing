#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import sys, os, psutil, plbufr
#import numpy as np
from glob import glob 
#import eccodes as ec        # bufr decoder by ECMWF
#import pandas as pd
#import polars as pl
#from collections import defaultdict
from datetime import datetime as dt, timedelta as td
from database import DatabaseClass as dc
from bufr import BufrClass as bc
from obs import ObsClass as oc
import global_functions as gf
import global_variables as gv
#import warnings
#warnings.simplefilter(action='ignore', category=FutureWarning)
#warnings.filterwarnings("ignore", module="plbufr")

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible


def decode_bufr( source=None, input_files=None, known_stations=frozenset(), pid_file=None ):
    #TODO
    """
    Parameter:
    ----------
    source : name of source (str)
    input_file : list of files to process
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
    PID             = os.getpid()
    status_locked   = f"locked_{PID}"

    if input_files:
        config_bufr = gf.merge_list_of_dicts( [config["Bufr"], config_script] )
        bf          = bc(config_bufr, script=script_name[-5:-3])

        if not args.source: source = "extra"
        else:               source = args.source

        db = dc(config=config_database)
        
        if not known_stations: known_stations = db.get_stations()

        FILES = {}

        for file_path in input_files:
            file_name   = file_path.split("/")[-1]
            file_date   = gf.get_file_date(file_path)
            bufr_dir    = "/".join(file_path.split("/")[:-1]) + "/"
            ID          = db.get_file_id(file_name, file_path)

            if ID:
                if not args.redo and db.get_file_status(ID) in bf.skip_status:
                    continue
                db.set_file_status(ID, status_locked)
            else:
                ID = db.register_file(file_name, file_path, source, status_locked, file_date, False, False)
                if not ID:
                    log.error(f"REGISTERING FILE '{file_path}' FAILED!")
                    continue

            FILES[ID] = { "name":file_name, "dir":bufr_dir, "date":file_date }

        db.close(commit=True)

    elif source:
        config_source = config_sources[source]
        if "bufr" in config_source:
             config_list = [ config["Bufr"], config_script, config_general, config_source["bufr"] ]
        else: return
        
        # previous dict entries will get overwritten by next list item during merge (right before left)
        config_bufr = gf.merge_list_of_dicts( config_list )

        bf = bc(config_bufr, script=script_name[-5:-3])

        bufr_dir = bf.dir + "/"

        try:    clusters = frozenset(config_source["clusters"])
        except: clusters = None

        db = dc(config=config_database)

        for i in range(max_retries):
            try:    known_stations = db.get_stations( clusters )
            except: pass
            else:   break
        
        if i == max_retries - 1: sys.exit(f"Can't access main database, tried {max_retries} times. Is it locked?")

        if hasattr(bf, "glob") and bf.glob: ext = f"{bf.glob}.{bf.ext}"
        else:                               ext = f"*.{bf.ext}" #TODO add possibility to use multiple extensions (set)
        
        if args.restart:
            files_to_parse = frozenset(db.get_files_with_status( f"locked_{args.restart}", source ))
        else:
            files_in_dir   = frozenset((os.path.basename(i) for i in glob( bufr_dir + ext )))

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

        FILES = {}

        for file_name in files_to_parse:
            
            file_path = gf.get_file_path( bufr_dir + file_name )
            file_date = gf.get_file_date( file_path )
            
            ID = db.get_file_id(file_name, file_path)
            if not ID:
                ID = db.register_file(file_name, file_path, source, status_locked, file_date, verbose=verbose)
                if not ID:
                    log.error(f"REGISTERING FILE '{file_path}' FAILED!")
                    continue

            FILES[ID] = { "name":file_name, "dir":bufr_dir, "date":file_date }
        
        db.close(commit=True)

        #TODO if multiprocessing: split files_to_parse by # of processes (e.g. 8) and parse files simultaneously
        #see https://superfastpython.com/restart-a-process-in-python/

    else: raise TypeError("Either source or input_files arguments have to be provided!")

    #TODO use defaultdic instead
    obs_bufr, file_statuses = {}, set()

    # initialize obs class (used for saving obs into station databases)
    # in this merge we are adding only already present keys; while again overwriting them
    config_obs  = gf.merge_list_of_dicts([config["Obs"], config_script], add_keys=False)
    obs         = oc( config_obs, source, mode=config_script["mode"] )

    for ID in FILES:
        
        new_obs         = 0
        obs_bufr[ID]    = {}
        FILE            = FILES[ID]
        PATH            = FILE["dir"] + FILE["name"]
        if verbose: print(PATH)
        
        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= bf.min_ram:
            
            db = dc(config=config_database)
            db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = bf.convert_keys_pd( obs_bufr, source, convert_datetime=False )
            if obs_db: obs.to_station_databases(obs_db)
            
            if pid_file: os.remove( pid_file )

            # see https://stackoverflow.com/a/33334183
            # cleanup file objects and descriptors before restart
            try:
                p = psutil.Process(PID)
                for handler in p.open_files() + p.connections():
                    os.close(handler.fd)
            except Exception as e:
                log.error(e)
            
            log.info("restart because of full memory")
            # get the name of the currently running executable
            exe = sys.executable
            # restart program with same arguments and add restart flag
            os.execl(exe, exe, * sys.argv, "-R", PID); sys.exit()


    db = dc(config=config_database)
    db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
    db.close(commit=True)
    
    if debug: print(obs_bufr)
    obs_db = bf.convert_keys_pd( obs_bufr, source, convert_datetime=False )

    if debug: print(obs_db)
    if obs_db: obs.to_station_databases(obs_db)
     
    # remove file containing the PID, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":

    from config import ConfigClass as cc

    info = """" Decode one or more BUFR files and insert relevant observation data into station databases.
                NOTE: Setting a command line flag or option always overwrites settings from the config file!"""

    script_name = gf.get_script_name(__file__)
    flags       = ("a","l","i","f","F","S","v","p","c","C","t","m","M","n","s","o","O","L","d","r","R")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, verbose=False)

    # get the right script name by adding approach suffix
    config_script   = cf.scripts[f"{script_name}_{cf.script['approach']}"]
    conda_env       = os.environ['CONDA_DEFAULT_ENV']
    
    if config_script["conda_env"] != conda_env:
        sys.exit(f"This script needs to run in conda environment {config_script['conda_env']}, exiting!")
    # import the right decode_bufr_?? function according to -a/--approach setting as decode_bufr
    decode_bufr = __import__( "decode_bufr_functions", globals(), locals(), [f"decode_bufr_{args.approach}"] )
    print(decode_bufr)
    sys.exit()

    # save the general part of the configuration in a variable for easier acces
    config_general = config["general"]

    if args.max_files is not None:  config_script["max_files"]  = args.max_files
    if args.sort_files:             config_script["sort_files"] = args.sort_files
    if args.pid_file:               config_script["pid_file"] = True
    
    if config_script["pid_file"]:
        pid_file = script_name + ".pid"
        if gf.already_running( pid_file ):
            sys.exit( f"{script_name} is already running... exiting!" )
    else: pid_file = None

    if args.profiler:
        config_script["profiler"] = args.profiler
    if config_script["profiler"]:
        from importlib import import_module
        profiler    = import_module(config_script["profiler"])
        profile     = True
    else: profile = False
    
    if args.log_level: config_script["log_level"] = args.log_level
    log = gf.get_logger(script_name)

    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"
    log.info(started_str)

    if args.verbose is not None:
        config_script["verbose"] = args.verbose
    
    verbose = config_script["verbose"]
    if verbose: print(started_str)
    
    if args.debug:                  config_script["debug"]          = True
    if config_script["debug"]:      import pdb; debug = True
    else:                           debug = False 

    if args.traceback:              config_script["traceback"]      = True

    if args.timeout:                config_script["timeout"]        = args.timeout
    
    if args.max_retries:            config_script["max_retries"]    = max_retries = int(args.max_retries)
    else:                           max_retries = int(config_script["max_retries"])

    if args.mode:                   config_script["mode"]           = args.mode
    if args.clusters:               config_source["clusters"]       = frozenset(args.clusters) 
        
    # get configuration for the initialization of the database class
    config_database = config["Database"]

    # add files table (file_table) to main database if not exists
    #TODO this should be done during initial system setup, file_table should be added there
    db = dc(config=config_database)
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    db.close()
    
    config_sources = None
    
    if args.file:
        # only processing a single BUFR file
        decode_bufr(source=args.source, input_files=[args.file], pid_file=pid_file)
    elif args.files:
        # input can be a semicolon-seperated list of files as well (or other seperator char defined by sep)
        #input_files = args.file.split(args.sep)
        if args.sep in args.file:
            import re
            input_files = re.split(args.sep, args.file)
        else: input_files = (args.file,)
        decode_bufr( source=args.source, input_files=input_files, pid_file=pid_file )
    
    elif args.source:
        source = config["sources"][args.source]

        if "," in source:
            sources = source.split(",")
            config_sources = {}
            for s in sources:
                config_sources[s] = config["sources"][s]

        else: config_sources = { args.source : config["sources"][args.source] }
   
    else: config_sources = config["sources"]
    
    if config_sources:
        for SOURCE in config_sources:
            if verbose: print(f"Parsing source {SOURCE}...")
            decode_bufr_gt( source = SOURCE, pid_file=pid_file )

    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)

    if verbose:
        print(finished_str)
    
    time_taken = stop_time - start_time
    print(f"{time_taken.seconds}.{time_taken.microseconds} s")
