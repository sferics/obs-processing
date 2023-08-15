#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import argparse, sqlite3, random, time, re, sys, os, psutil, shelve, time, pdbufr
import logging as log
from copy import copy
import random, time
import numpy as np
from glob import glob 
import eccodes as ec        # bufr decoder by ECMWF
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", module="pdbufr")
from pathlib import Path    # path operation
from datetime import datetime as dt, timedelta as td
from database import database; import global_functions as gf; import global_variables as gv

#TODO write more (inline) comments, docstrings and make try/except blocks much shorter where possible

def text_factory( value ):
    if type(value) == pd.Timestamp:
        return value.to_pydatetime()
    else: return value

to_datetime = lambda meta : dt(meta["year"], meta["month"], meta["day"], meta["hour"], meta["minute"])

clear   = lambda keyname : str( re.sub( r"#[0-9]+#", '', keyname ) )
number  = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key  = lambda key,num : "#{num}#{key}"


def translate_key( key, value, duration, h=None ):

    key_db = bufr_translation[key]

    if key_db is None: return None, None, None
    if h is not None: # we are looking for a specific height/depth key, key_db is a dict now
        try:    key_db = key_db[h]
        except  KeyError:
            print("height ERROR")
            print(key, value, h)
            return None, None, None
    else: bufr_translation[key]

    # add units + scale conversion
    value = float(value) * key_db[2] + key_db[3]

    # some keys always have the same duration (like pressure always 1s)
    #print(fixed_duration); sys.exit()
    #if key in fixed_duration: duration = key_db[1]
    # if we already got a timePeriod from BUFR we dont need to translate it with the yaml dict
    if not duration or key in fixed_duration_keys:  duration = key_db[1]
    if duration is None:                            duration = "NULL"

    return key_db[0], value, duration


def convert_keys( obs, dataset ):

    time_periods = bufr_translation["timePeriod"]

    if debug: print(obs)
    obs_db = {}
    #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

    for file in obs:
        
        for location in obs[file]:
            
            if location not in obs_db: obs_db[location] = set()
            
            for datetime in obs[file][location]:
                
                datetime_db     = datetime.to_pydatetime()
                duration_obs    = ""
                vertical_sigf   = 0
                clouds_present  = False
                cloud_cover     = None
                cloud_ceiling   = float("inf")
                cloud_amounts, cloud_bases  = set(), set()
                sensor_height, sensor_depth = None, None

                for time_period in obs[file][location][datetime]:
                   
                    if len(obs[file][location][datetime][time_period]) == 1 and obs[file][location][datetime][time_period][-1][0] in gv.modifier_keys:
                        continue

                    try:
                        if time_period: duration_obs = time_periods[time_period]
                    except: continue
                   
                    for data in obs[file][location][datetime][time_period]:
                        
                        key, val_obs = data[0], data[1]

                        #if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                        #else:                           datetime_db = copy(datetime)

                        if key == "verticalSignificanceSurfaceObservations":
                            vertical_sigf = bufr_flags[key][val_obs]; continue
                        elif key == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform":
                            sensor_height = float(val_obs); continue
                        elif key == "depthBelowLandSurface":
                            sensor_depth  = float(val_obs) * (-1); continue
                        else:
                            if key in height_depth_keys:
                                if key == "soilTemperature":
                                    h = copy(sensor_depth)
                                    if not h or h > 0: h = -0.05
                                else:
                                    h = copy(sensor_height)
                                    if not h or h > 1: h = 2.0

                                element, val_db, duration = translate_key(key, val_obs, duration_obs, h=h)
                            elif key in {"heightOfBaseOfCloud","cloudCoverTotal","cloudAmount"}:
                                clouds_present = True
                                if key == "heightOfBaseOfCloud":
                                    # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                    if val_obs < cloud_ceiling: cloud_ceiling = copy(val_obs)

                                    # also we want to save all unique cloud levels (base heights) which where observed
                                    cloud_bases.add(val_obs); continue

                                elif key == "cloudCoverTotal":
                                    cloud_cover = copy(val_obs)
                                    element, val_db, duration = translate_key(key, cloud_cover, duration_obs)
                                    if dataset in {"DWD","test"}: val_db = int(val_db)

                                #elif key == "cloudAmount":
                                else:
                                    cloud_amounts.add( val_obs )
                                    if not vertical_sigf: continue
                                    element, val_db, duration = translate_key(key, val_obs, duration_obs, h=vertical_sigf)
                                
                            else: element, val_db, duration = translate_key(key, val_obs, duration_obs)
                            if element is not None:
                                obs_db[location].add( ( dataset, file, datetime_db, duration, element, val_db ) )
                            else: print(f"element is None for key: {key}, value: {val_obs}")


                if clouds_present:
                    if cloud_ceiling < float("inf"):
                        key = "heightOfBaseOfCloud"
                        element, val_db, duration = translate_key(key, cloud_ceiling, duration_obs )
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = translate_key("cloudAmount", max(cloud_amounts), duration_obs, h=0)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = translate_key("cloudBase", cloud_base, duration_obs, h=i+1)
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                        cloud_bases = set()

    
    return obs_db


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
        ext             = config_bufr["ext"]
         
        if "filter" in config_bufr:
            filter_keys             = set(config_bufr["filter"])
            number_of_filter_keys   = len(filter_keys)

            fun = lambda x : not pd.isnull(x);      filters     = {}
            for i in filter_keys:                   filters[i]  = fun
        else: filters = {}; number_of_filter_keys = float("inf")

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
    new_obs = 0

    for FILE in files_to_parse:
        
        ID = file_IDs[FILE]
        obs[ID] = {}
        
        PATH = bufr_dir + FILE

        #TODO for some reason my nice NaN removal filter doesnt work; fix or let it be...
        #df = pdbufr.read_bufr(PATH, filters=filters, columns="all", flat=True, required_columns=required_keys)
        #df = pdbufr.read_bufr(PATH, columns="all", flat=True, required_columns=required_keys)
        #df = pdbufr.read_bufr(PATH, flat=True, columns="data")
        #print(df)
        df = pdbufr.read_bufr(PATH, columns=relevant_keys, required_columns=required_keys)

        # len(df.index) == 0 is much faster than df.empty or len(df) == 0
        # https://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty
        if len(df.index) == 0: file_statuses.add( ("empty", ID) ); continue
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

        for ix, row in df.iterrows():

            try:
                if pd.notna(row["timePeriod"]): time_period = row["timePeriod"]
            except: pass 

            try:
                if time_period == -1 and row[gv.replication] == 10 and (pd.notna(row["presentWeather"]) or pd.notna(row["totalPrecipitationOrTotalWaterEquivalent"])):
                    continue
            except: pass
            
            location = str(row["WMO_station_id"]) + "0"
            if location not in known_stations: continue
            datetime = row["data_datetime"]
            if not datetime: sys.exit("NO DATETIME")
            for i in (gv.replication, "timePeriod", "WMO_station_id", "data_datetime"):
                try:    del row[i]
                except: continue

            #keys_not_na = relevant_obs.intersection(row.index)
            # in future versions of pandas we will need this next line:
            #keys_not_na = list(relevant_obs.intersection(row.index))

            #if row.loc[keys_not_na].isna().all(): continue
            
            #keys_not_na = relevant_obs.intersection(row.index)
            #if not row.loc[keys_not_na].notna().any(): continue
                
            if location not in obs[ID]:             obs[ID][location]           = {}
            if datetime not in obs[ID][location]:   obs[ID][location][datetime] = {}

            modifier_list = []
            for key in (gv.sensor_height, gv.sensor_depth, gv.vertical_sigf):
                try:    
                    if pd.notna(row[key]): modifier_list.append((key, row[key]))
                except: continue
                else:   del row[key]

            obs_list = []
            for key, val in zip(row.index, row):
                if pd.notna(val): obs_list.append((key, val))

            if modifier_list and obs_list: obs_list = modifier_list + obs_list
            if obs_list:
                try:    obs[ID][location][datetime][time_period] += obs_list
                except: obs[ID][location][datetime][time_period] = obs_list

        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            
            db = database(db_file, timeout=timeout_db, traceback=traceback)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
            db.close()

            print("Too much RAM used, RESTARTING...")
            obs_db = convert_keys( obs, source )
            if obs_db: gf.obs_to_station_databases( obs_db, output_path, "raw", max_retries, timeout_station, verbose=verbose )
            
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()

    db = database(db_file, timeout=timeout_db, traceback=traceback)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
    db.close()

    obs_db = convert_keys( obs, source )

    if obs_db: gf.obs_to_station_databases(obs_db, output_path, "raw", max_retries, timeout_station, verbose=verbose)
     
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
    conda_env       = os.environ['CONDA_DEFAULT_ENV']
    
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
    if verbose: print("\n"+started_str)

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

    gf.create_dir(output_path+"/raw")

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
    meta_keys = (gv.sensor_height,gv.sensor_depth,gv.vertical_sigf)
    modifier_keys, depth_keys, height_keys, fixed_duration_keys = set(), set(), set(), set()

    fixed_durations = {"0s","1s","1min"}

    for i in bufr_translation:
        if type(bufr_translation[i]) == dict:
            
            try:    subkey = list(bufr_translation[i])[0]
            except: continue

            if bufr_translation[i][subkey][1] in fixed_durations:
                fixed_duration_keys.add(i)
            
            if type(subkey) == float:
                if subkey   < 0: depth_keys.add(i)
                elif subkey > 0: height_keys.add(i)
        
        elif type(bufr_translation[i]) == list:
            if bufr_translation[i][1] in fixed_durations:
                fixed_duration_keys.add(i)
        
        elif type(bufr_translation[i]) == type(None): modifier_keys.add(i)

    modifier_keys.add("timePeriod")

    # union of both will be used later
    height_keys, depth_keys = frozenset(height_keys), frozenset(depth_keys)
    height_depth_keys       = frozenset( height_keys | depth_keys )
    
    required_keys = frozenset({"WMO_station_id","data_datetime"})
    relevant_keys = frozenset( bufr_obs_time_keys | required_keys )
    relevant_obs  = set(bufr_translation_keys) - gv.modifier_keys - {gv.replication}

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
