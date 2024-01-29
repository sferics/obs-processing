#!/usr/bin/env python
import os
import sys
from datetime import datetime as dt, timedelta as td
from database import database_class
from obs import obs_class
import global_functions as gf

#TODO implement source/dataset priority order OR use scale column! for now, just stick with dataset test
#dataset = "test"

def reduce_obs(stations):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    # for st in stations:
    # get only data rows with highest file ID and copy the remaining data to forge databases
    for loc in stations:
        db_file = f"{output_path}/{mode}/{loc[0]}/{loc}.db"
        try: db_loc = database_class( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue

        obs.create_station_tables(loc)

        match mode:
            case "dev":
                sql = ["INSERT INTO obs_forge SELECT DISTINCT dataset,file,datetime,duration,element,value FROM obs_raw WHERE reduced=0"]
                sql.append("UPDATE obs_raw SET reduced=1")
                
            case "oper":
                sql = ["INSERT INTO obs_forge f SELECT DISTINCT dataset,file,datetime,duration,element,value FROM obs_raw r WHERE reduced=0 AND cor = ( SELECT MAX(cor) FROM obs_raw r WHERE r.datetime=f.datetime AND r.element=f.element AND r.duration=f.duration AND file = ( SELECT MAX(file) FROM obs_raw  WHERE r.datetime=f.datetime AND r.element=f.element AND r.duration=f.duration"]
                sql.append("UPDATE obs_raw SET reduced = 1")

            case "test":
                pass

        #https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column
        # 3 statements to get all data and copy them to a new database (forge)

        #gf.create_station_tables( loc, output_path, "forge", verbose=verbose )
        
        #TODO the CREATE TABLE command below already creates the table; is there another solution?
        # problem: we might want/need the actual table structure from station_tables_forge
        
        #sql = [f"ATTACH DATABASE '{output_path}/{mode}/{loc[0]}/{loc}.db' AS {mode}"]
        
        #https://stackoverflow.com/questions/57134793/how-to-save-query-results-to-a-new-sqlite
        #oper mode when we want to keep all CORs
        #sql.append("CREATE TABLE IF NOT EXISTS forge.obs AS SELECT DISTINCT a.datetime,a.duration,a.element,a.value FROM main.obs a WHERE cor = ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration ) AND file = ( SELECT MAX(file) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )")
        #dev mode when we only want to keep latest COR
        #sql.append(f"CREATE TABLE IF NOT EXISTS {mode}.obs_forge AS SELECT DISTINCT datetime,duration,element,value FROM main.obs WHERE reduced=0")
        #sql.append(f"CREATE UNIQUE INDEX unique_obs ON {mode}.obs_forge(datetime,duration,element)")
        #sql.append(f"DETACH {mode}")
       
        #"DELETE FROM main.obs a WHERE cor < ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )"
        #"UPDATE main.obs a set reduced=1 WHERE cor < ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )"

        for sql in sql:
            if verbose: print(sql)
            try: db_loc.exe(sql)
            except Exception as e:
                if verbose: print(e)
        
        db_loc.close()

    return


if __name__ == "__main__":

    script_name     = gf.get_script_name(__file__)
    config          = gf.read_yaml( "config" )
    config_script   = config["scripts"][script_name]
    output_path     = config_script["output_path"]
    verbose         = config_script["verbose"]
    traceback       = config_script["traceback"]
    debug           = config_script["debug"]
    mode            = config["general"]["mode"]
    
    if "mode" in config_script:
        mode = config_script["mode"]

    obs             = obs_class( typ="forge", mode=mode, config=config_script, source="test" )
    cluster         = set( config_script["clusters"].split(",") )
    db              = database_class( config=config["database"], ro=1 )
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
            p = mp.Process(target=reduce_obs, args=(station_group,))
            p.start()

    else: reduce_obs(stations)
