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
from database import database_class
import global_functions as gf
#import global_variables as gv

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible

clear = lambda keyname : str( re.sub( r"#[0-9]+#", '', keyname ) )
number          = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key          = lambda key,num : "#{num}#{key}"


def get_bufr( bufr, key, number=None ):
    if type(key) == tuple:
        return ec.codes_get( bufr, f"#{key[0]}#{key[1]}" )
    elif number: return ec.codes_get( bufr, f"#{number}#{key}" )
    else:       return ec.codes_get( bufr, key )

station_keys    = { "stationNumber", "blockNumber", "shortStationName" }
mandatory_keys  = { "stationOrSiteName", "latitude", "longitude" }
additional_keys = { "elevation", "heightOfStation", "heightOfStationGroundAboveMeanSeaLevel" }

relevant_keys   = station_keys | mandatory_keys | additional_keys | { "subsetNumber" }

#TODO in future we could add the following keys:
#future_keys = {"heightOfBarometerAboveMeanSeaLevel", "nationalStationNumber", "stationType", "stateIdentifier"}

#TODO rewrite to add_new_stations( metas ) which uses executemany()
def add_new_station( meta ):
    """
    Parameter:
    ----------
    meta : dict containing the station meta information (location, lat, lon, elev, ...)

    Notes:
    ------
    helper function, adds the station to the station_table of the main.db

    Return:
    -------
    None
    """
    loc, lat, lon = meta["location"], meta["latitude"], meta["longitude"]
    
    known_stations.add(loc)

    if "elevation" not in meta and "heightOfStationGroundAboveMeanSeaLevel" in meta:
        elev = meta["heightOfStationGroundAboveMeanSeaLevel"]
    elif "heightOfStation" in meta:
        elev = meta["heightOfStation"]
    else: elev = "NULL"

    #TODO replace "germany" by a cluster variable
    station_data = ( loc, "", meta["stationOrSiteName"], lon, lat, elev, "germany", "" )

    if verbose: print("Adding", meta["stationOrSiteName"], "to database...")

    db = database_class(db_file, timeout=timeout_db)
    db.add_station( station_data, verbose=False )
    db.close(commit=True)


def scan_all_BUFRs_for_stations( source, known_stations, pid_file=None ):
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
    config_source  = config_sources[source]
    config_bufr    = config_source["bufr"]
    bufr_dir       = config_bufr["dir"] + "/"
    ext            = config_bufr["ext"]

    if "glob" in config_bufr and config_bufr["glob"]:   ext = f"{config_bufr['glob']}.{ext}"
    else:                                               ext = f"*.{ext}" #TODO add multiple extensions (list)
    
    files_to_parse  = set((os.path.basename(i) for i in glob( bufr_dir + ext )))

    gf.create_dir( bufr_dir )

    station_types = set(config_source["stations"].split(","))

    for FILE in files_to_parse:

        with open(bufr_dir + FILE, "rb") as f:
            try:
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    #if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                ec.codes_set(bufr, "skipExtraKeyAttributes", 1) # we dont need units and so on for this purpose
                ec.codes_set(bufr, "unpack", 1)
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

            for skip_function in ( ec.codes_skip_computed, ec.codes_skip_function, ec.codes_skip_duplicates ):
                skip_function( iterid )

            meta            = {}
            location        = None
            skip_next       = 0
            subset          = 0
            valid_loc       = False

            while ec.codes_bufr_keys_iterator_next(iterid):
                
                if skip_next: skip_next -= 1; continue

                keyname   = ec.codes_bufr_keys_iterator_get_name(iterid)
                clear_key = clear(keyname)
                
                if clear_key not in relevant_keys: continue

                if keyname == "subsetNumber":
                    if subset > 0:
                        if meta: add_new_station(meta)
                        meta = {}; location = None; valid_loc = False
                    subset += 1
                    continue
                
                if location is None and clear_key in station_keys:
                    try: meta[clear_key] = ec.codes_get(bufr, keyname)
                    except: meta[clear_key] = ec.codes_get_array(bufr, keyname)[0]
                    #TODO some OGIMET-BUFRs seem to contain multiple station numbers
                    
                    if meta[clear_key] in null_vals: del meta[clear_key]
                    
                    # check for identifyer of DWD station (nebenamtliche Stationen)
                    if "shortStationName" in meta:
                        location = meta["location"] = meta["shortStationName"]
                        station_type = "dwd"
                    # check if all essential station information keys for a WMO station are present
                    elif { "stationNumber", "blockNumber" }.issubset( set(meta) ):
                        location = str(meta["stationNumber"] + meta["blockNumber"] * 1000).rjust(5,"0") + "0"
                        meta["location"] = location
                        station_type = "wmo"
                    else: continue

                elif location and location not in known_stations and station_type in station_types:
                    if clear_key in mandatory_keys:
                        meta[clear_key] = ec.codes_get(bufr, keyname)
                        if meta[clear_key] in null_vals: del meta[clear_key]
                    if mandatory_keys.issubset(set(meta)): 
                       valid_loc = True
                else:
                    meta = {}; location = None
                    continue
                
                if valid_loc:
                    if clear_key in additional_keys:
                        meta[clear_key] = ec.codes_get(bufr, keyname)
                        if meta[clear_key] in null_vals: del meta[clear_key]
                        continue
            
            # end of while loop
            if meta: add_new_station(meta)
            ec.codes_keys_iterator_delete(iterid)
       
        # end of with clause (closes file handle)
        ec.codes_release(bufr) # frees some memory TODO still there is a little leak somewhere...

    # remove file containing the pid, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":
    
    #pid_file = config_script["pid_file"]

    #if gf.already_running( pid_file ):
    #    sys.exit( f"{sys.argv[0]} is already running... exiting!" )
    
    #read yaml configuration file config.yaml into dictionary
    config = gf.read_yaml( "config.yaml" )
    config_script   = config["scripts"][sys.argv[0]]
    #if config_script["profile"]: import cProfiler #TODO use module
    #if config_script["logging"]: import logging   #TODO use module
    verbose         = config_script["verbose"]

    if verbose: print(f"STARTED {sys.argv[0]} @ {dt.utcnow()}")

    null_vals = { ec.CODES_MISSING_LONG, ec.CODES_MISSING_DOUBLE } # (2147483647, -1e+100)
    for i in config_script["null_vals"]: null_vals.add( i )

    #station_keys    = set(config_script["station_keys"])
    traceback       = config_script["traceback"]
    timeout_db      = config["database"]["timeout"]
    db_file         = config["database"]["db_file"]

    max_retries     = config_script["max_retries"]

    db = database_class(db_file, timeout=timeout_db)

    retries = copy(max_retries)
    while retries > 0:
        try: known_stations = db.get_stations()
        except: retries -= 1
        else: break
    
    db.close()
    if retries == 0: sys.exit(f"Cannot access main database, tried {max_retries} times... Is it locked?")

    #parse command line arguments
    if len(sys.argv) >= 2:
        source = config["sources"][sys.argv[1]]

        if "," in source:
            sources = source.split(","); config_sources = {}
            for s in sources: config_sources[s] = config["sources"][s]

        else: config_sources = { sys.argv[1] : config["sources"][sys.argv[1]] }
    else: config_sources = config["sources"]

    for SOURCE in config_sources:
        if verbose: print(f"Parsing source {SOURCE}...")
        #scan_all_BUFRs_for_stations( SOURCE, known_stations, pid_file )
        scan_all_BUFRs_for_stations( SOURCE, known_stations )

    if verbose: print(f"FINISHED {sys.argv[0]} @ {dt.utcnow()}")
