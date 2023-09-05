#!/usr/bin/env python
import os
#import numpy as np
from operator import itemgetter
from itertools import groupby
from collections import defaultdict
import global_functions as gf
import global_variables as gv
import sql_factories as sf
from database import database_class


def delete_duplicate_obs(stations):
    
    for loc in stations:

        print("\n",loc)

        sql_values = set()
        
        db_file = f"{output_path}/{loc[0]}/{loc}.db"
        try: db_loc = database_class( db_file )
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
    
   #TODO implement source option! for now, just stick with test
    src = "test"

    config          = gf.read_yaml( "config.yaml" )
    db_settings     = config["database"]["settings"]
    script_name     = gf.get_script_name(__file__)
    config_script   = config["scripts"][script_name]
    output_path     = config["output_path"]
    verbose         = config_script["verbose"]
    traceback       = config_script["traceback"]

    db              = database_class( config=config["database"] )

    clusters        = set(config_script["clusters"].split(","))
    stations        = db.get_stations( clusters ); db.close(commit=False)

    if config_script["multiprocessing"]:
        # number of processes
        npcs = config_script["multiprocessing"]
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, npcs)
        station_groups = np.array_split(stations, npcs)

        for station_group in station_groups:
            p = mp.Process(target=delete_duplicate_obs, args=(station_group,))
            p.start()

    else: delete_duplicate_obs(stations)
