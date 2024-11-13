#!/usr/bin/env python
import sys
import os
import gzip
import csv
import pandas as pd
from datetime import datetime as dt
from obs import ObsClass as oc
from database import DatabaseClass as dc
from config import ConfigClass as cc
import global_functions as gf


def import_metwatch(stations):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    def translate_metwatch_IDs(metwatchID):                            # comment from metwatch.cfg
        """
        Purpose:
        Translates the metwatch IDs to the standard amalthea nomenclature.

        Parameter:
        metwatchID - element name 

        Notes:
        Also extracts the duration from the lookup table. 
        Returns a tuple of the amalthea nomenclature and the duration of the element.

        """

        return metwatch_import[metwatchID.strip()]
    
    def parse_metwatch(loc):
        """
        Purpose:
        Provides the index_map that is later used to identify which column to acquire from the bufr files.

        Parameter:
        available_obs - all elements from the element_table whose role is "obs".

        Notes:
        Maps the elements from the amalthea nomenclature with those from the bufr file. 

        """
        sql_values      = set()
        
        # get all bufr elements
        try:
            fhand = gzip.open(f"{input_dir}/bufr{loc}.csv.gz", mode="rt")
        except FileNotFoundError:
            return None
        else:
            reader = csv.reader( fhand, delimiter=';' )
            if verbose: print(f"{input_dir}/bufr{loc}.csv.gz")
            # acquire indexes
            # go through file line by line
            while True:
                try:
                    line = next(reader)
                    # if we encounter INDEX as the fir (datetime, duration, element, val) )
                except Exception as e:
                    print(e)
                    break
                else:
                    # if we encounter INDEX as the first element, we know it's a header line
                    if line[0].strip() == "INDEX":
                        continue
                    else:
                        if verbose: print(line[:-1])
                        # else we parse the line element by element to save the values
                        for idx, val in enumerate(line[:-1]):
                            if verbose:
                                print("idx, val, len(metwatch_header)")
                                print(idx, val, len(metwatch_header))
                            if idx in mw_relevant_pos:
                                if idx == 8: # datetime
                                    Y, M, D, h, m = val[:4], val[4:6], val[6:8], val[8:10], val[10:12]
                                    datetime = dt(int(Y), int(M), int(D), int(h), int(m))
                                else:
                                    val         = val.strip()
                                    mw_element  = mw_header_keys[idx]
                                    translation = translate_metwatch_IDs(mw_element)
                                    element, duration, multiply, add_to, replace = translation
                                    if verbose:
                                        print("element, duration, multiply, add_to, replace")
                                        print(element, duration, multiply, add_to, replace)
                                    if val != "/":
                                        if replace is not None and val in replace:
                                            if verbose:
                                                print("val, replace, replace[va]")
                                                print(val, replace, replace[val])
                                            val = replace[val]
                                        else:
                                            if multiply is not None:
                                                try:    val = float(val) * multiply
                                                except: pass
                                            if add_to is not None:
                                                try:    val = float(val) + add_to
                                                except: pass
                                        if verbose:
                                            print("datetime, duration, element, val")
                                            print(datetime, duration, element, val)
                                        sql_values.add( (datetime, duration, element, val) )
            fhand.close()
            
        return sql_values
    

    for loc in stations:
        
        db_file = obs.get_station_db_path(loc)
        if verbose: print(db_file)
        obs.create_station_tables(loc)
        #db_file = f"{output}/{mode}/forge/{loc[0]}/{loc}.db"
        try: db_loc = dc( db_file, {"verbose":verbose, "traceback":traceback}, ro=False )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue
         
        sql_insert = ("INSERT INTO obs (dataset,datetime,duration,element,value) "
            "VALUES('metwatch',?,?,?,?) ON CONFLICT DO ")
        if update: # update flag which forces already existing values to be updated
            sql_insert += "UPDATE SET value = excluded.value, duration = excluded.duration"
        else: # if -u/--update flag is not set do nothing
            sql_insert += "NOTHING"
        
        #TODO import relevant data from csv files in legacy output directory and insert to database
        sql_values = parse_metwatch(loc) 
        
        if sql_values is not None:
            if verbose: print(loc, len(sql_values))
            db_loc.exemany(sql_insert, sql_values)
            db_loc.commit()
        
        db_loc.close(commit=False)
        
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Import legacy observations (metwatch csv) into Amalthea station databases"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","E","u","r")
    cf          = cc(script_name, flags=flags, info=info, clusters=True)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    
    started_str, start_time = gf.get_started_str_time(script_name)
    log.info(started_str)

    # define some shorthands from script config
    verbose         = cf.script["verbose"]
    debug           = cf.script["debug"]
    traceback       = cf.script["traceback"]
    timeout         = cf.script["timeout"]
    max_retries     = cf.script["max_retries"]
    mode            = cf.script["mode"]
    output          = cf.script["output"]
    input_dir       = cf.script["input"]
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]
    update          = cf.script["update"]
    extra           = cf.script["extra"] if cf.script["extra"] else "metwatch"
    redo            = cf.args.redo

    metwatch_transl = gf.read_yaml("translations/metwatch")
    metwatch_header = metwatch_transl["header"]
    mw_header_keys  = tuple(metwatch_header.keys())
    metwatch_import = metwatch_transl["import"]
    # remember all needed elements (keys of metwatch_import dict + datetime)
    mw_relevant_ele = set(metwatch_import.keys()) | {"YYYYMMDDhhmm"}
    # get all relevant positions where we encounter a needed header element 
    mw_relevant_pos = { idx for idx, ele in enumerate(mw_header_keys) if ele in mw_relevant_ele }
    
    obs             = oc( cf, source=extra, mode=mode, stage="raw", verbose=verbose )
    db              = dc( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )

    db.close(commit=False)

    if processes: # number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=import_metwatch, args=(station_group,))
            p.start()

    else: import_metwatch(stations)
