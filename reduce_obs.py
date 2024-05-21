#!/usr/bin/env python
import os
import sys
import sqlite3
from datetime import datetime as dt, timedelta as td
from database import DatabaseClass
from obs import ObsClass
from config import ConfigClass
import global_functions as gf
import global_variables as gv

#TODO implement source/dataset priority order OR use scale column! for now, just stick with dataset test
#dataset = "test"

def reduce_obs(stations):
    """
    Parameter:
    ----------
    stations : list of stations to consider

    Notes:
    ------

    Return:
    -------
    None
    """
    # for st in stations:
    # get only data rows with highest file ID and copy the remaining data to forge databases
    for loc in stations:
        db_file = f"{output}/raw/{loc[0]}/{loc}.db"
        try: db_loc = DatabaseClass( db_file, {"verbose":verbose, "traceback":traceback}, ro=False )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue

        # create tation table in the forge databases directory
        obs.create_station_tables(loc)
        # attach forge database to fill it with reduced observational data
        db_loc.attach(f"{output}/forge/{loc[0]}/{loc}.db", "forge")
        #db_loc.drop_table("forge.obs", exists=False)
        sql = "DROP TABLE forge.obs;\n"
        
        # if debug: mark all data as not reduced again and thereby process anew
        if debug: sql += "UPDATE obs SET reduced = 0;\n"

        match mode:
            case "dev":
                # in dev mode we only need to reduce to one (datetime,duration,element,value)
                # by selecting only the highest priority source (and from that the highest scale)
                
                sql += ("CREATE TABLE forge.obs AS SELECT DISTINCT "
                    "dataset,datetime,duration,element,value FROM main.obs r WHERE reduced=0 "
                    "AND prio = ( SELECT MAX(prio) FROM main.obs WHERE "
                    "r.dataset=obs.dataset AND r.datetime=obs.datetime "
                    "AND r.duration=obs.duration AND r.element=obs.element );\n")
                
            case "oper":
                #TODO debug! this looks crazy and might also not be necessary (just use dev's sql?)
                # select latest COR (correction) of highest scale from source with highest prio
                sql += ("CREATE TABLE forge.obs AS SELECT DISTINCT "
                    "dataset,datetime,duration,element,value FROM main.obs r WHERE reduced=0 "
                    "AND prio = ( SELECT MAX(prio) FROM main.obs WHERE cor = ( SELECT "
                    "MAX(cor) FROM main.obs WHERE scale = ( SELECT "
                    "MAX(scale) FROM main.obs WHERE r.dataset=obs.dataset AND "
                    "r.datetime=obs.datetime AND r.duration=obs.duration "
                    "AND r.element=obs.element ) ) );\n")
                """
                sql += ("CREATE TABLE forge.obs AS SELECT DISTINCT "
                    "dataset,datetime,duration,element,value FROM main.obs r WHERE reduced=0 "
                    "AND prio = ( SELECT MAX(prio) FROM main.obs WHERE r.datetime=obs.datetime "
                    "AND r.element=obs.element AND r.duration=obs.duration AND cor = ( SELECT "
                    "MAX(cor) FROM main.obs WHERE r.dataset=obs.dataset, r.datetime=obs.datetime 
                    "AND r.element=obs.element AND r.duration=obs.duration AND scale = ( SELECT "
                    "MAX(scale) FROM main.obs WHERE r.dataset=obs.dataset AND r.datetime=obs.datetime "
                    "AND r.duration=obs.duration AND r.element=obs.element ) ) );\n")
                """
                
            case "test":
                raise NotImplementedError("TODO")
        
        # detach the forge database again
        sql += "DETACH forge;\n"
        # mark all data as reduced (processed)
        sql += "UPDATE obs SET reduced = 1;"

        #if debug: print(sql)

        try: db_loc.exescr(sql)
        except sqlite3.Error as e:
            if verbose:     print(e)
            if traceback:   gf.print_trace(e)
        else: db_loc.close()
        
    # currently the function does not return any value (None)
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Reduce the number of observations according to operation mode"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P")
    cf          = ConfigClass(script_name, pos=["source"], flags=flags, info=info, clusters=True)
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
    output          = cf.script["output"] + "/" + mode
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]

    obs             = ObsClass( cf, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)

    if processes: # use number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=reduce_obs, args=(station_group,))
            p.start()

    else: reduce_obs(stations)
