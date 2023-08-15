#!/usr/bin/env python

import sys
from datetime import datetime as dt, timedelta as td
from database import database
import global_functions as gf

config          = gf.read_yaml( "config.yaml" )
db_settings     = config["database"]["settings"]
config_script   = config["scripts"][sys.argv[0]]
verbose         = config_script["verbose"]
traceback       = config_script["traceback"]
debug           = config_script["debug"]
#if debug: import pdb

db              = database( config["database"]["db_file"],ro=1,verbose=0,traceback=1,settings=db_settings )

cluster         = config_script["station_cluster"]
stations        = db.get_stations( cluster ); db.close(commit=False)

#TODO implement source/dataset priority order OR use scale column! for now, just stick with dataset test
#dataset = "test"

def reduce_obs(stations):

    # for st in stations:
    # get only data rows with highest file ID and copy the remaining data to forge databases
    for loc in stations:

        sql_values = set()

        try: db_loc = database( f"/home/juri/data/stations_pd/raw/{loc[0]}/{loc}.db", ro=True, verbose=True, traceback=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            #if debug:       pdb.set_trace()
            continue

        # statement to get all desired data of raw database (WORKING? TODO CHECK!)
        #https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column
        sql_select = "SELECT DISTINCT a.element,a.value,a.duration,a.datetime FROM obs a \
        INNER JOIN ( SELECT element, value, duration, datetime, MAX(file) file FROM obs GROUP BY \
        element, value, duration, datetime ) b on a.element = b.element AND a.value = b.value \
        AND a.duration=b.duration AND a.datetime=b.datetime AND a.file=b.file WHERE a.file >= 0"

        #gf.create_dir(f'/home/juri/data/stations/forge/{loc[0]}')

        #TODO 3 statements to get all data and copy them to a new database (forge)

        gf.create_station_tables( loc, "/home/juri/data/stations", "forge", verbose=verbose )

        sql = [f"ATTACH DATABASE '/home/juri/data/stations/forge/{loc[0]}/{loc}.db' AS forge"]
        
        #https://stackoverflow.com/questions/57134793/how-to-save-query-results-to-a-new-sqlite
        """
        sql.append("CREATE TABLE forge.obs AS SELECT a.datetime,a.element,a.value,a.duration FROM main.obs a \
        INNER JOIN ( SELECT element, value, duration, datetime, MAX(file) file FROM main.obs GROUP element,value,duration ) b ON a.element = b.element AND a.duration=b.duration AND \
        a.datetime=b.datetime AND a.value=b.value, AND a.file=b.file WHERE a.file > 0")
        """
        """
        sql.append("CREATE TABLE forge.obs AS SELECT a.datetime,a.element,a.value,a.duration FROM main.obs a INNER JOIN ( SELECT datetime,element,value,duration,file FROM main.obs ) b ON a.file=(SELECT MAX(file) FROM main.obs WHERE a.element = b.element AND a.duration=b.duration AND \
        a.datetime=b.datetime AND a.file > 0 AND b.file > 0)")
        """
        sql.append("CREATE TABLE forge.obs AS SELECT a.datetime,a.element,a.value,a.duration FROM main.obs a WHERE file = ( SELECT MAX(file) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration AND a.file>0 AND b.file>0 )")
        sql.append("DETACH forge;")
        
        #print(";\n".join(sql))
        
        for sql in sql:
            try:    db_loc.exe(sql)
            except Exception as e:
                print(e); pass
        
        if verbose:
            print(loc)
            print(db_loc.rowcnt)
            #print(db_loc.fetch()) 

    return


if __name__ == "__main__":

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
            p = mp.Process(target=reduce_obs, args=(station_group,))
            p.start()

    else: reduce_obs(stations)
