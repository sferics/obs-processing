#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and add new stations to main database

from copy import copy
import random, time
import numpy as np
from glob import glob 
import eccodes as ec        # bufr decoder by ECMWF
import sqlite3              # python sqlite connector for error handling (database lock)
import re, sys, os, psutil  # regular expressions, system, operating system, process handling
from pathlib import Path    # path operation
from datetime import datetime as dt, timedelta as td
from config import ConfigClass as cc
from database import DatabaseClass as dc
from bufr import BufrClass as bc
from obs import ObsClass as oc
import global_functions as gf
import global_variables as gv

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible

clear   = lambda keyname : str( re.sub( r"#[0-9]+#", '', keyname ) )
number  = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key  = lambda key,num : "#{num}#{key}"


def get_bufr( bufr, key, number=None ):
    if type(key) == tuple:
        return ec.codes_get( bufr, f"#{key[0]}#{key[1]}" )
    elif number:    return ec.codes_get( bufr, f"#{number}#{key}" )
    else:           return ec.codes_get( bufr, key )

station_keys    = { "stationNumber", "blockNumber", "shortStationName" }
mandatory_keys  = { "stationOrSiteName", "latitude", "longitude" }
additional_keys = { "elevation", "heightOfStation", "heightOfStationGroundAboveMeanSeaLevel" }

relevant_keys   = station_keys | mandatory_keys | additional_keys | { "subsetNumber" }

#TODO in future we could add the following keys:
#future_keys = {"heightOfBarometerAboveMeanSeaLevel", "nationalStationNumber", "stationType", "stateIdentifier"}

# counter generator for WMO stations
def counter(start=0):
    """Generator function that counts up from 1 to infinity on each call."""
    # make this an iterator that starts at a given number
    i = start
    # use a while loop to yield an infinite sequence of numbers
    while True:
        # yield the current number and increment it
        yield i
        # if you want to start counting from 1, uncomment the next line
        i += 1

# create a counter instance
counter = counter(1)  # starts counting from 1


def scan_all_BUFRs_for_stations( cf, input_files_dict={}, SOURCE="extra", pid_file=None ):
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
    if not input_files_dict: 
        print(f"No input files found for source '{SOURCE}'")
        return
    
    #TODO use defaultdic instead for obs_bufr ???
    obs_bufr, file_statuses = {}, set()
    # initialize obs class (used for saving obs into station databases)
    obs = oc( cf, SOURCE, verbose=verbose )
    # initialize bufr class (contains all bufr specifics contants and settings)
    bf  = bc( cf, SOURCE )

    stations = set()

    for ID in input_files_dict:
        
        FILE        = input_files_dict[ID]
        FILE_DIR    = FILE["dir"]
        FILE_NAME   = FILE["name"]
            
        with open(FILE_DIR+FILE_NAME, "rb") as f:
            try:
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                #ec.codes_set(bufr, "skipExtraKeyAttributes", 1) # we dont need units and so on for this purpose
                ec.codes_set(bufr, "unpack", 1)
                # force processing of all subsets
                subsets = ec.codes_get_long( bufr, "numberOfSubsets" )
                if subsets > 0:
                    ec.codes_set(bufr, "extractSubsetIntervalStart", 1);
                    ec.codes_set(bufr, "extractSubsetIntervalEnd", subsets)
                    ec.codes_set(bufr, "doExtractSubsets", 1)
            except Exception as e:
                if traceback: gf.print_trace(e)
                if verbose: print(f"ERROR:  '{FILE}'")
                continue
            
            iterid = ec.codes_bufr_keys_iterator_new(bufr)

            #for skip_function in ( ec.codes_skip_computed, ec.codes_skip_function, ec.codes_skip_duplicates ):
            #    skip_function( iterid )

            meta            = {}
            location        = None
            station_type    = None
            subset          = 0
            valid_loc       = False

            while ec.codes_bufr_keys_iterator_next(iterid):
                 
                keyname   = ec.codes_bufr_keys_iterator_get_name(iterid)
                clear_key = clear(keyname)
                
                if SOURCE in {"NCAR", "NCEP"}:
                    print(f"DEBUG: {keyname} -> {clear_key}")
                    arr = ec.codes_get_array(bufr, keyname)
                    if arr[0] == "WMOS     WMO STATION NUMBER     ":
                        stations.add( next(counter) )
                        print(stations)
                        ec.codes_bufr_keys_iterator_next(iterid)
                        keyname   = ec.codes_bufr_keys_iterator_get_name(iterid)
                        arr = ec.codes_get_array(bufr, keyname)
                        print(arr[0])
                    continue
        
                if clear_key not in relevant_keys: continue

                if keyname == "subsetNumber":
                    if subset > 0:
                        if station_type in cf.script["stations"] and location:
                            stations.add(location)
                        meta = {}; location = None; valid_loc = False
                    subset += 1
                    continue
                
                if location is None and clear_key in station_keys:
                    try:    meta[clear_key] = ec.codes_get(bufr, keyname)
                    except:
                        meta[clear_key] = ec.codes_get_array(bufr, keyname)[0]
                    #TODO some OGIMET-BUFRs seem to contain multiple station numbers
                    
                    if meta[clear_key] in null_vals: del meta[clear_key]
                    
                    # check for identifyer of DWD station (nebenamtliche Stationen)
                    if "shortStationName" in meta:
                        location = meta["location"] = meta["shortStationName"]
                        station_type = "dwd"
                    # check if all essential station information keys for a WMO station are present
                    elif { "stationNumber", "blockNumber" }.issubset( set(meta) ):
                        location = str(meta["stationNumber"] + meta["blockNumber"] * 1000).rjust(5,"0") #+ "0"
                        meta["location"] = location
                        station_type = "wmo"
                    else:
                        station_type = None
                        continue
                    
                    if station_type in cf.script["stations"] and location:
                        stations.add(location)
                        meta     = {}
                        location = None
                        continue
            
            # end of while loop
            if SOURCE not in {"NCAR","NCEP"} and station_type in cf.script["stations"] and location:
                stations.add( location )
            ec.codes_keys_iterator_delete(iterid)
       
        # end of with clause (closes file handle)
        ec.codes_release(bufr) # frees some memory TODO still there is a little leak somewhere...

    if verbose:
        for station in sorted(stations): print(station)
    
    n_input_files = len(input_files_dict)
    if n_input_files > 1:
        print(f"Found {len(stations)} stations in {n_input_files} BUFR files from source '{source}'.")
    
    # remove file containing the pid, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":

    # define program info message (--help, -h)
    info        = "Count number of stations found in BUFR file(s)"
    script_name = gf.get_script_name(__file__)
    flags       = ("f","F","l","v","C","m","M","o","O","d","t","P")
    cf          = cc(script_name, ["source"], flags=flags, info=info, sources=True)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"

    log.info(started_str)

    # define some shorthands from script config
    pid_file        = cf.script["pid_file"]
    verbose         = cf.script["verbose"]
    debug           = cf.script["debug"]
    traceback       = cf.script["traceback"]
    timeout         = cf.script["timeout"]
    max_retries     = cf.script["max_retries"]
    mode            = cf.script["mode"]
    output          = cf.script["output"] + "/" + mode
    stations        = cf.script["stations"]
    source          = cf.script["source"]
    args            = cf.args

    obs             = oc( cf, source, stage="forge" )
    db              = dc( config=cf.database, ro=1 )
    stations        = db.get_stations()
    db.close(commit=False)

    null_vals = { ec.CODES_MISSING_LONG, ec.CODES_MISSING_DOUBLE } # (2147483647, -1e+100)
    for i in cf.script["null_vals"]: null_vals.add( i )

    db = dc(config=cf.database)

    retries = copy(max_retries)
    while retries > 0:
        try: known_stations = db.get_stations()
        except: retries -= 1
        else: break
    
    db.close()
    if retries == 0:
        sys.exit(f"Cannot access main database, tried {max_retries} times... Is it locked?")

    config_sources   = None

    if args.file or args.files:
        if args.file:
            # only processing a single BUFR file
            input_files_dict = gf.get_input_files_dict(cf.database, [args.file], redo=True,
                restart=False, verbose=verbose)
        else:
            # input can be a semicolon-seperated list of files as well (or other divider char)
            input_files         = args.files.split(args.divider)
            input_files_dict    = gf.get_input_files_dict(cf.database, input_files, redo=True,
                restart=False, verbose=verbose)

        if debug: print(input_files_dict)
        scan_all_BUFRs_for_stations( cf, input_files_dict, source, pid_file=pid_file )

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
                    redo=True, restart=False, verbose=verbose )
            scan_all_BUFRs_for_stations( cf, input_files_dict, SOURCE, pid_file )

    if verbose: print(f"FINISHED {sys.argv[0]} @ {dt.utcnow()}")
