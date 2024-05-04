#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import sys, os, psutil, copy
#from collections import defaultdict
from datetime import datetime as dt, timedelta as td
from database import DatabaseClass as dc
from bufr import BufrClass as bc
from obs import ObsClass as oc
import global_functions as gf
import global_variables as gv
#import decode_bufr_approaches

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible


def decode_bufr( cf, input_files_dict={}, source="extra", approach="gt", pid_file=None, verbose=False ):
    #TODO
    """
    Parameter:
    ----------
    source : name of source (default: extra)
    cf : config class object
    input_files_dict : dict containing all files to process and all their important meta information
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
    #TODO use defaultdic instead
    obs_bufr, file_statuses = {}, set()

    # initialize obs class (used for saving obs into station databases)
    obs = oc( cf, source, verbose=verbose )
    # initialize bufr class (contains all bufr specifics contants and settings)
    bf  = bc( cf, source, approach=approach )

    convert_keys = getattr(bf, f"convert_keys_{approach}")
    
    #TODO move this into BufrClass ???
    db = dc(config=cf.database)
    for i in range(bf.max_retries):
        try:    known_stations = db.get_stations( bf.clusters )
        except: pass
        else:   break
    db.close()

    if i == bf.max_retries - 1: known_stations = None

    setattr(bf, "known_stations", known_stations)

    new_obs = 0

    for ID in input_files_dict:
        
        FILE        = input_files_dict[ID]
        FILE_DIR    = FILE["dir"]
        FILE_NAME   = FILE["name"]

        if verbose: print(FILE_DIR + FILE_NAME)

        obs_bufr[ID], file_status = decoder_approach(ID, FILE_NAME, FILE_DIR, bf, log, traceback, debug, verbose)
        
        file_statuses.add( (file_status, ID) )

        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= bf.min_ram:
            
            db = dc(config=cf.database)
            db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = convert_keys( obs_bufr, source, shift_dt=shift_dt, convert_dt=convert_dt )
            if obs_db: obs.to_station_databases(obs_db, scale=scale_info)
            
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


    db = dc(config=cf.database)
    db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
    db.close(commit=True)
    
    if debug: print(obs_bufr)
    obs_db = convert_keys( obs_bufr, source, shift_dt=shift_dt, convert_dt=convert_dt )

    if debug: print(obs_db)
    if obs_db: obs.to_station_databases(obs_db, scale=scale_info)
     
    # remove file containing the PID, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":

    from config import ConfigClass as cc

    info = """  Decode one or more BUFR files and insert relevant observation data into station databases.
                NOTE: Setting a command line flag or option always overwrites settings from the config file!"""

    script_name = gf.get_script_name(__file__)
    flags       = ("a","l","i","E","f","F","D","v","p","c","C","t","k","m","M","n","s","o","O","L","d","r","R","w")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, verbose=False)
    # get the right script name by adding approach suffix
    script_name = cf.script_name

    # get currently active conda environment
    conda_env   = os.environ['CONDA_DEFAULT_ENV']
    
    # check whether script is running in correct environment; if not exit
    if cf.script["conda_env"] != conda_env:
        sys.exit(f"This script ({script_name}) needs to run in conda env {cf.script['conda_env']}, exiting!")

    # get a logger instance for the current script
    log = gf.get_logger(script_name)

    # remeber the starting time of the script so we can measure its performance later (we do this quite late...)
    start_time  = dt.utcnow()
    # a string that can be printed to log or stdout
    started_str = f"STARTED {script_name} @ {start_time}"
    log.info(started_str)

    # save PID, we will need it later
    PID = str(os.getpid())

    # shorthands for cli arguments and config
    args        = cf.args
    approach    = args.approach 
    verbose     = cf.script["verbose"]
    traceback   = cf.script["traceback"]
    debug       = cf.script["debug"]
    pid_file    = cf.script["pid_file"]
    scale_info  = True if approach in cf.script["scale_info"]       else False
    shift_dt    = True if approach in cf.script["shift_datetime"]   else False
    convert_dt  = True if approach in cf.script["convert_datetime"] else False

    if args.no_warnings:
        import warnings
        warnings.filterwarnings("ignore")

    # get the right decode_bufr_?? function according to -a/--approach setting as decoder_approach
    #decoder_approach = getattr(decode_bufr_approaches, f"decode_bufr_{approach}")
    decoder_approach = gf.import_from("decode_bufr_approaches", f"decode_bufr_{approach}", globals(), locals())
     
    config_sources = None
    
    if args.file or args.files:
        if verbose and args.redo: print("REDO")
        if args.file:
            # only processing a single BUFR file
            input_files_dict = gf.get_input_files_dict( cf.database, [args.file], PID=PID, redo=args.redo )
        
        else:
            # input can be a semicolon-seperated list of files as well (or other seperator char defined by sep)
            input_files = args.files.split(args.sep)
            #if args.sep in args.files:
            #    import re
            #    input_files = re.split(args.sep, args.file)
            #else: input_files = args.files
            input_files_dict = gf.get_input_files_dict( cf.database, input_files, PID=PID,
                    redo=args.redo, verbose=verbose )
       
        if debug: print(input_files_dict)
        decode_bufr( cf, input_files_dict, cf.args.extra, approach, pid_file, verbose=verbose )
    
    elif args.source:
        if len(args.source) > 1:
            config_sources = {}
            for s in args.source:
                config_sources[s] = cf.sources[s]

        else: config_sources = { args.source[0] : cf.sources[args.source[0]] }
   
    else: config_sources = cf.sources
   
    if config_sources:
        for SOURCE in config_sources:
            if verbose: print(f"Parsing source {SOURCE}...")
            config_source = cf.sources[SOURCE]
            if verbose: print(f"CONFIG: {config_source}")
            if "bufr" in config_source:
                config_source = cf.general | cf.bufr | cf.script | config_source["bufr"]
            else: continue
            
            input_files_dict = gf.get_input_files_dict( cf.database, source=SOURCE,
                    config_source=config_source, PID=PID, verbose=verbose )
            decode_bufr( cf, input_files_dict, SOURCE, approach, pid_file, verbose=verbose )

    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)

    if verbose: print(finished_str)
   
    gf.print_time_taken(start_time, stop_time)
