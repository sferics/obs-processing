#!/usr/bin/env python
import os
import sys
import re
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
        db_file = obs.get_station_db_path(loc)
        try: db_loc = DatabaseClass( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue
        
        obs.create_station_tables(mode=mode, stage="final")
        db_loc.attach_station_db(loc, output, mode=mode, stage="final")
        
        #sql = ""
        sql_good    = "INSERT INTO obs.final (timestamp,element,value) VALUES (?,?,?)"
        sql_bad     = "INSERT INTO obs_bad.final (timestamp,element,value,reason) VALUES (?,?,?,?)"
        values_good, values_bad = set(), set()

        for element in elements:
            # element properties and range
            element_info_el                 = element_info[element]
            lower, upper, extra, exclude    = ( element_info_el[i] for i in range(3) )
            element_range                   = range(lower, upper)

            # 1 get all 30-min data for this element
            data = db_loc.exe((f"SELECT datetime,element,value FROM obs WHERE element='{element}' "
                f"AND strftime('%M', datetime) IN ('00','30')"))
            # 2 check for bad (out-of-range) values
            
            for row in data:
                #sql = ""
                
                datetime, element, val = row[0], row[1], row[2]
                
                if (val in element_range or val in extra) and val != exclude:
                    # 3b insert good data into obs table of final database
                    #sql += f"INSERT INTO obs.final (timestamp,element,value) VALUES ({row[0]},{row[1]},{row[2]})\n"
                    datetime = int( datetime.timestamp() )
                    values_good.add(row)
                else:
                    try:
                        # if the value does match the exclude pattern it gets excluded
                        if re.match(exclude, str(val)):
                            reason = "excluded"
                        elif val < lower:
                            reason = "too_low"
                        elif val > upper:
                            reason = "too_high"
                        # if it is neither to low nor to high it must be also not an extra value
                        else: #elif val not in extra:
                            reason = "not_extra"
                    except TypeError:
                        # TypeError can occur while comparing str with float / int using "<" or ">"
                        reason = "not_numeric"
                    
                    # 3b insert bad data into obs_bad table of final database
                    #sql += f"INSERT INTO obs_bad.final (timestamp,duration,element,value,reason) VALUES ({row[0]},{row[1]},{row[2]},{reason})\n"
                    timestamp = int( datetime.timestamp() )
                    values_bad.add((timestamp, element, val, reason))
                        
            
        db_loc.exemany(sql_good, values_good)
        db_loc.exemany(sql_bad, values_bad)
        #db_loc.exescr(sql)
        db_loc.close(commit=True)

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

    obs             = ObsClass( cf, source, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)

    elements        = tuple(f'{element}' for element in db.get_elements())
    element_info    = gf.read_yaml(cf.script["element_info"])

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
