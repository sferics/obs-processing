#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import argparse, sqlite3, re, sys, os, psutil#, shelve
import logging as log
import numpy as np
from glob import glob
import metview as mv
import pandas as pd
from pathlib import Path    # path operation
from datetime import datetime as dt, timedelta as td
from database import database_class
import global_functions as gf
import global_variables as gv
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
        
        if "tables" in config_bufr:
            os.putenv('METVIEW_EXTRA_GRIB_DEFINITION_PATH', config_bufr["tables"])
        
        try:    clusters = set(config_source["clusters"].split(","))
        except: clusters = None

        db = database_class(db_file, timeout=timeout_db, traceback=traceback)
        
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

        db = database_class(db_file, timeout=timeout_db, traceback=traceback)
        known_stations  = db.get_stations()

        ID = db.get_file_id(FILE, file_path)
        if ID:  db.set_file_status(ID,"locked")
        else:   ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)

        db.close(commit=True)

        file_IDs = {FILE:ID}

    obs, file_statuses = {}, set()
    new_obs = 0

    for FILE in files_to_parse:
        
        ID = file_IDs[FILE]
        obs[ID] = {}
        
        PATH=file_path+"/"+FILE
        try:
            #mv.cleanfile(path=PATH)
            data = mv.read(source=PATH)
            #data = mv.cleanfile(data=data)
            
            #data = mv.bufr_picker(data=data, parameter=relevant_keys[:5], missing_data="ignore", fail_on_error="no", output="ncols")
            
            #https://vocabulary-manager.eumetsat.int/vocabularies/WMO-Common/WMO/Current/C13
            data = mv.obsfilter(data=data, parameter=relevant_keys, date_and_time_from="data",output="ncols", fail_on_error="yes", fail_on_empty_output="no")
            #cats    = 0
            #subcats = (0,1,2,6,7)
            
            #data = mv.obsfilter(data=data, parameter=relevant_keys, date_and_time_from="data", observation_types=cats, observation_subtypes=subcats, output="geopoints", fail_on_error="yes", fail_on_empty_output="no")#, wmo_stations=tuple(known_stations))
        except Exception as e:
            gf.print_trace(e)
            continue
        else:
            #print(mv.stnids(data))
            #df = data.to_dataframe()
            #print(df.describe())
            print(data.columns())
            print(data.values())
            print(data.value_columns())
            #print(data.value2s())

        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            
            db = database_class(db_file, timeout=timeout_db, traceback=traceback)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = convert_keys_se( obs, source, modifier_keys, height_depth_keys, bufr_translation, bufr_flags )
            if obs_db: gf.obs_to_station_databases( obs_db, output_path, max_retries, timeout_station, verbose )
            
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()


    db = database_class(db_file, timeout=timeout_db, traceback=traceback)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
    db.close()

    obs_db = convert_keys_se( obs, source, modifier_keys, height_depth_keys, bufr_translation, bufr_flags )

    if obs_db: gf.obs_to_station_databases(obs_db, output_path, max_retries, timeout_station, verbose)
     
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
    
    """
    if config_script["conda_env"] != conda_env:
        sys.exit(f"This script needs to run in conda environment {config_script['conda_env']}, exiting!")
    """

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
    
    if debug: os.environ['METVIEW_PYTHON_DEBUG'] = "1"
    else:     os.environ['METVIEW_PYTHON_DEBUG'] = "0" 

    if args.traceback:  traceback = True
    else:               traceback = config_script["traceback"]
    
    null_vals = { i for i in config_script["null_vals"] }

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
    db = database_class(db_file, timeout=timeout_db, traceback=traceback)
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    db.close()

    # parse the BUFR translation and bufr flags yaml files into dictionaries
    bufr_translation    = gf.read_yaml( config_script["bufr_translation"] )
    time_periods        = bufr_translation["timePeriod"]
    bufr_flags          = gf.read_yaml( config_script["bufr_flags"] )

    # get the keys from the conversion dictionary, skip the first 7 which are used for unit conversions and stuff
    relevant_keys = tuple( (str(i).rjust(6, "0") for i in tuple(bufr_translation)[7:]) )

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

    # union of both will be used later
    height_keys, depth_keys = frozenset(height_keys), frozenset(depth_keys)
    height_depth_keys       = frozenset( height_keys | depth_keys )

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
