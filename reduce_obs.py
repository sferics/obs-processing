#!/usr/bin/env python
import os
import sys
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

    Notes:
    ------

    Return:
    -------

    """
    # for st in stations:
    # get only data rows with highest file ID and copy the remaining data to forge databases
    for loc in stations:
        db_file = f"{output}/raw/{loc[0]}/{loc}.db"
        try: db_loc = DatabaseClass( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue

        # create tation table in the forge databases directory
        #obs.create_station_tables(loc)
        #sql = [f"ATTACH DATABASE '{output}/forge/{loc[0]}/{loc}.db' AS forge"]

        match mode:
            case "dev":
                sql = f"ATTACH DATABASE '{output}/forge/{loc[0]}/{loc}.db' AS forge;\n"
                #sql = ["INSERT INTO forge.obs SELECT DISTINCT dataset,file,datetime,duration,element,value FROM main.obs WHERE reduced=0"]
                #sql.append("UPDATE main.obs SET reduced=1")
                # in dev mode we do not perform this forging stage (happens during inserting to database to save time)
                # instead, we only select all distinct values and create the obs tables in all forge databases
                sql += "CREATE TABLE IF NOT EXISTS forge.obs AS SELECT DISTINCT datetime,duration,element,value FROM main.obs;\n"
                sql += "DETACH forge;"

            case "oper":
                obs.create_station_tables(loc)
                sql = "INSERT INTO forge.obs f SELECT DISTINCT dataset,file,datetime,duration,element,value FROM main.obs r WHERE reduced=0 AND cor = ( SELECT MAX(cor) FROM obs_raw r WHERE r.datetime=f.datetime AND r.element=f.element AND r.duration=f.duration AND file = ( SELECT MAX(file) FROM main.obs WHERE r.datetime=f.datetime AND r.element=f.element AND r.duration=f.duration;"
                # delete all but latest COR
                #sql.append("DELETE FROM main.obs a WHERE cor < ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )")
                #sql.append("UPDATE main.obs SET reduced = 1")
                # keep all CORs
                sql += "UPDATE main.obs a set reduced=1 WHERE cor < ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration );"
                
                #sql.append("CREATE TABLE IF NOT EXISTS forge.obs AS SELECT DISTINCT a.datetime,a.duration,a.element,a.value FROM main.obs a WHERE cor = ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration ) AND file = ( SELECT MAX(file) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )")
                
            case "test":
                raise NotImplementedError("TODO")

        #https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column
        # 3 statements to get all data and copy them to a new database (forge)

        #gf.create_station_tables( loc, output_path, "forge", verbose=verbose )
        
        #TODO the CREATE TABLE command below already creates the table; is there another solution?
        # problem: we might want/need the actual table structure from station_tables_forge
        
        #sql = [f"ATTACH DATABASE '{output}/forge/{loc[0]}/{loc}.db' AS forge"]
        #sql.append("CREATE TABLE IF NOT EXISTS forge.obs AS SELECT DISTINCT a.datetime,a.duration,a.element,a.value FROM main.obs a WHERE cor = ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration ) AND file = ( SELECT MAX(file) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )")

        #https://stackoverflow.com/questions/57134793/how-to-save-query-results-to-a-new-sqlite
        #oper mode when we want to keep all CORs
        #sql.append("CREATE TABLE IF NOT EXISTS forge.obs AS SELECT DISTINCT a.datetime,a.duration,a.element,a.value FROM main.obs a WHERE cor = ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration ) AND file = ( SELECT MAX(file) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )")
        #dev mode when we only want to keep latest COR
        #sql.append(f"CREATE TABLE IF NOT EXISTS {mode}.obs_forge AS SELECT DISTINCT datetime,duration,element,value FROM main.obs WHERE reduced=0")
        #sql.append(f"CREATE UNIQUE INDEX unique_obs ON {mode}.obs_forge(datetime,duration,element)")
        #sql.append(f"DETACH {mode}")
       
        #"DELETE FROM main.obs a WHERE cor < ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )"
        #"UPDATE main.obs a set reduced=1 WHERE cor < ( SELECT MAX(cor) FROM main.obs b WHERE a.datetime=b.datetime AND a.element=b.element AND a.duration=b.duration )"
        
        """
        for sql in sql:
            if verbose: print(sql)
            try: db_loc.exe(sql)
            except Exception as e:
                if verbose: print(e)
        """

        try: db_loc.exescr(sql)
        except Exception as e:
            if verbose:     print(e)
            if traceback:   gf.print_trace(e)
        else: db_loc.close()

    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Reduce the number of observations according to operation mode"
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

    obs             = ObsClass( config=cf.obs, source=source, mode=mode, stage="forge" )
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
