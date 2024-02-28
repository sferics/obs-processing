#!/usr/bin/env python
import sys
import os
from obs import ObsClass
from database import DatabaseClass
from config import ConfigClass
import global_functions as gf


def conclude_obs(stations):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    for loc in stations:
        db_file = f"{output_path}/forge/{loc[0]}/{loc}.db"
        try: db_loc = DatabaseClass( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue

        #OBS = obs(loc, {"output_path":output_path, "typ":"forge", "verbose":verbose})
        #OBS.create_station_tables( loc, verbose=verbose )
        gf.create_dir( f"{output_path}/final/{loc[0]}" )

        sql = [f"ATTACH DATABASE '{output_path}/final/{loc[0]}/{loc}.db' AS final"]
        #sql.append(f"CREATE TABLE final.obs AS SELECT DISTINCT datetime,duration,element,value FROM main.obs WHERE element IN{elements} AND strftime('%M', datetime) IN ('00','30')")
        # convert datetime to timestamp (seconds after UNIX), inspired by: https://stackoverflow.com/questions/27545543/how-can-i-convert-a-datetime-string-to-a-unix-timestamp-in-sqlite3
        sql.append(f"CREATE TABLE final.obs AS SELECT DISTINCT CAST(strftime('%s', strftime('%Y-%m-%dT00:00:00+00:00', datetime), 'unixepoch') AS datetime), duration, element, value FROM main.obs WHERE element IN{elements} AND strftime('%M', datetime) IN ('00','30')")
        #sql.append("CREATE UNIQUE INDEX unique_obs ON dev.obs(datetime,duration,element)") 
        sql.append("DETACH dev")
        
        for sql in sql:
            if verbose: print(sql)
            try: db_loc.exe(sql)
            except Exception as e:
                if verbose: print(e)

        db_loc.close()
    
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Finish forging and save remaining obs to 'final' databases"
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
    elements        = tuple(f'{element}' for element in db.get_elements())
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
            p = mp.Process(target=conclude_obs, args=(station_group,))
            p.start()

    else: conclude_obs(stations)
