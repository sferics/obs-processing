#!/usr/bin/env python
import os
import sys
from datetime import datetime as dt, timedelta as td
from database import DatabaseClass
from config import ConfigClass
from obs import ObsClass
import global_functions as gf


def audit_obs(stations):
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
    
    # 1 take data from forge databases
    for loc in stations:
        db_file = f"{output_path}/forge/{loc[0]}/{loc}.db"
        try: db_loc = DatabaseClass( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue
        
        for element in elements:
            
            # get all data for this element
            data = db_loc.exe(f"SELECT * FROM obs WHERE element='{element}'")
            # 2 check for bad (out-of-range) values
            
            for row in data:
                
                # element range of element
                er_element      = element_ranges[element]
                element_range   = range(er_element[0], er_element[1])
                sql = []
                
                if (row[3] in element_range or row[3] == er_element[2]) and row[3] != er_element[3]:
                    # 3a round value to significant digits (defined by scale)
                    pass
                else:
                    reason = ""
                    
                    if row[3] < lower:
                        reason = "to_low"
                    elif row[3] > upper:
                        reson = "to_high"
                    
                    # 3b delete bad data -> move to obs_bad database
                    sql.append(f"DELETE FROM obs WHERE datetime='{row[0]}' AND duration='{row[1]}' AND element='{row[2]}' AND value='{row[3]}'")
                    sql.append(f"INSERT INTO obs_bad (datetime,duration,element,value,reason) VALUES ({row[0]},{row[1]},{row[2]},{row[3]},{reason})")

                for s in sql:
                    print(s)
                    db_loc.exe(s)


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

    elements        = tuple(f'{element}' for element in db.get_elements())
    element_ranges  = gf.read_yaml(cf.script["element_ranges"])

    if processes: # number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=audit_obs, args=(station_group,))
            p.start()

    else: audit_obs(stations)
