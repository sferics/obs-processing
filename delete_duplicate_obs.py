#!/usr/bin/env python
import os
#import numpy as np
from operator import itemgetter
from itertools import groupby
from collections import defaultdict
import global_functions as gf
import global_variables as gv
import sql_factories as sf
from obs import ObsClass
from database import DatabaseClass
from config import ConfigClass


def delete_duplicate_obs(stations):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """    
    for loc in stations:

        print("\n",loc)

        sql_values = set()
        
        db_file = f"{output_path}/{loc[0]}/{loc}.db"
        try: db_loc = DatabaseClass( db_file )
        except Exception as e:
            gf.print_trace(e)
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue

        # https://sqlite.org/forum/info/ad235e398f502740e74e3485d64d6d391922de1dec34bdf7b463cd6a84fd8105
        sql = "DELETE FROM obs WHERE rowid NOT IN(SELECT max(rowid) from obs GROUP BY datetime,duration,element,value)"
        try:    db_loc.exe(sql)
        except: continue

        if verbose:
            sql = ["SELECT * FROM obs WHERE rowid NOT IN(SELECT max(rowid) from obs GROUP BY datetime,duration,element,value)"]
            sql.append("SELECT * FROM obs")
            for sql in sql:
                try:    db_loc.exe(sql)
                except: continue

        sql = "CREATE UNIQUE INDEX IF NOT EXISTS unique_obs ON obs(datetime,duration,element)"
        try:    db_loc.exe(sql)
        except Exception as e:
            gf.print_trace(e)
            continue

        db_loc.close(commit=True)
    
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Delete duplicate entries in station databases (should actually no be necessary)"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P")
    cf          = ConfigClass(script_name, pos=["source"], flags=flags, info=info, verbose=True)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"

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

    obs             = ObsClass( cf, source, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
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
            p = mp.Process(target=delete_duplicate_obs, args=(station_group,))
            p.start()

    else: delete_duplicate_obs(stations)
