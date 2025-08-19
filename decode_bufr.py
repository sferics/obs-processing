#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import sys, os, psutil, copy
#from collections import defaultdict
from datetime import datetime as dt, timedelta as td
from config import ConfigClass as cc
from database import DatabaseClass as dc
from bufr import BufrClass as bc
from obs import ObsClass as oc
import global_functions as gf
import global_variables as gv


def decode_bufr( cf, input_files_dict={}, SOURCE="extra", approach="gt", pid_file=None, verbose=False ):
    """
    Parameter:
    ----------
    source : name of source (default: extra)
    cf : config class object
    input_files_dict : dict containing all files to process and all their important meta information
    pid_file : name of the file where the process id gets stored (str)
    
    Notes:
    ------
    main function of the script, parses all files of given source and tries to save them in database
    using the obs.to_station_databases() function (from the obs class).
    includes file handling and sets the status of a file to 'locked' before starting to handle it,
    to 'empty' if no (relevant) data was found in it, to status 'error' if something went wrong
    (which should not occur but we never know...) or - if everything went smooth - 
    to status == 'parsed' in the file_table of the main database. pid_file is optional
    
    Return:
    -------
    None
    """
    #TODO use defaultdic instead for obs_bufr ???
    obs_bufr, file_statuses = {}, set()
    # initialize obs class (used for saving obs into station databases)
    obs = oc( cf, SOURCE, verbose=verbose )
    # initialize bufr class (contains all bufr specifics contants and settings)
    bf  = bc( cf, SOURCE, approach=approach )
    # get the right conver_keys function depending on approach 
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
         
        if debug: print(FILE_DIR + FILE_NAME)
        
        obs_bufr[ID], file_status = decoder_approach(ID, FILE_NAME, FILE_DIR, bf, log, traceback,
                debug, verbose) 
        file_statuses.add( (file_status, ID) )
        
        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= bf.min_ram:
                
            db = dc(config=cf.database)
            db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
            db.close()
            
            obs_db = convert_keys( obs_bufr, SOURCE, shift_dt=shift_dt, convert_dt=convert_dt )
            
            if obs_db:      obs.to_station_databases(obs_db, scale=scale_info)
            if pid_file:    os.remove( pid_file )
            if verbose:     print("Too much RAM used, RESTARTING...")
            
            try:# to cleanup file objects and descriptors (see https://stackoverflow.com/a/33334183)
                p = psutil.Process(PID)
                for handler in p.open_files() + p.connections(): os.close(handler.fd)
            except Exception as e:
                log.error(e)
            
            log.info("restart because of full memory")
            
            # remove old restart argument if present
            argv = sys.argv
            if "-R" in argv:
                R_idx = argv.index("-R")
                del argv[R_idx:R_idx+2]
            
            # get the name of the currently running executable
            exe = sys.executable
            # restart program with same arguments and add restart flag
            os.execl(exe, exe, *argv, "-R", PID)
            sys.exit("RESTART")
            
        
    db = dc(config=cf.database)
    db.set_file_statuses(file_statuses, retries=bf.max_retries, timeout=bf.timeout)
    db.close(commit=True)
    
    if debug: print(obs_bufr)
    obs_db = convert_keys( obs_bufr, SOURCE, shift_dt=shift_dt, convert_dt=convert_dt, verbose=verbose )
    
    if debug: print(obs_db)
    if obs_db: obs.to_station_databases(obs_db, scale=scale_info, verbose=verbose)
      
    # remove file containing the PID, so the script can be started again
    if pid_file: os.remove( pid_file )
    

if __name__ == "__main__":
    
    info = """  Decode BUFR file(s) and insert all relevant observation data into station databases.
                NOTE:
                Setting a command line flag or option always overwrites settings from config file!
                """
    
    script_name = gf.get_script_name(__file__)
    flags       = ("a","l","i","E","f","F","D","v","p","c","C","t","k","m","M","n","s","o","O","L",
            "d","r","R","w")
    cf          = cc(script_name, ["source"], flags=flags, info=info, sources=True, clusters=True)
    script_name = cf.script_name
    
    # get currently active conda environment
    conda_env   = os.environ['CONDA_DEFAULT_ENV']
    
    # check whether script is running in correct environment; if not exit
    if cf.script["conda_env"] != conda_env:
        sys.exit(f"{script_name} needs to run in conda env {cf.script['conda_env']} - exiting!")
    
    # get a logger instance for the current script
    log = gf.get_logger(script_name)
    
    # get a string that can be printed to log or stdout and start time
    started_str, start_time = gf.get_started_str_time(script_name)
    log.info(started_str)
    
    # save PID, we will need it later
    PID = str(os.getpid())
    
    # define shorthands for command line arguments and config
    args        = cf.args
    approach    = cf.script["approach"]
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
    gls, lcs         = globals(), locals()
    decoder_approach = gf.import_from("decode_bufr_approaches", f"decode_bufr_{approach}", gls, lcs)
    config_sources   = None
   
    if args.file or args.files:
        if args.file:
            # only processing a single BUFR file
            input_files_dict = gf.get_input_files_dict( cf.database, [args.file], PID=PID,
                    redo=args.redo, restart=args.restart, verbose=verbose )
        else:
            # input can be a semicolon-seperated list of files as well (or other divider char)
            input_files         = args.files.split(args.divider)
            input_files_dict    = gf.get_input_files_dict( cf.database, input_files, PID=PID,
                    redo=args.redo, restart=args.restart, verbose=verbose )
         
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
                    
            input_files_dict = gf.get_input_files_dict( cf.database, None, SOURCE, config_source,
                    PID=PID, redo=args.redo, restart=args.restart, verbose=verbose )
            decode_bufr( cf, input_files_dict, SOURCE, approach, pid_file, verbose=verbose )
        
    finished_str = gf.get_finished_str(script_name)

    log.info(finished_str)
    if verbose: print(finished_str)
    gf.print_time_taken(start_time, precision=3)
