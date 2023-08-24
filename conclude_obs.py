#!/usr/bin/env python
import sys
import os
from database import database
import global_functions as gf

def conclude_obs(stations):

    for loc in stations:
        db_file = f"/home/juri/data/stations/forge/{loc[0]}/{loc}.db"
        try: db_loc = database( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue

        gf.create_dir( f"/home/juri/data/stations/dev/{loc[0]}" )

        sql = [f"ATTACH DATABASE '/home/juri/data/stations/dev/{loc[0]}/{loc}.db' AS dev"]
        sql.append("CREATE TABLE dev.obs AS SELECT DISTINCT datetime,duration,element,value FROM main.obs")
        sql.append("DETACH dev")
        
        for sql in sql:
            if verbose: print(sql)
            try: db_loc.exe(sql)
            except Exception as e:
                if verbose: print(e)

        db_loc.close()
    
    return


if __name__ == "__main__":

    script_name     = os.path.basename(__file__)
    config          = gf.read_yaml( "config.yaml" )
    config_script   = config["scripts"][script_name]
    verbose         = config_script["verbose"]
    traceback       = config_script["traceback"]
    debug           = config_script["debug"]
    #if debug: import pdb

    cluster         = set( config_script["clusters"].split(",") )
    db              = database( config=config["database"], ro=1 )
    stations        = db.get_stations( cluster ); db.close(commit=False)

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
            p = mp.Process(target=conclude_obs, args=(station_group,))
            p.start()

    else: conclude_obs(stations)
