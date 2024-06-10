#!/usr/bin/env python
import os
import sys
import re
from datetime import datetime as dt, timedelta as td
from database import DatabaseClass as dc
from config import ConfigClass as cc
from obs import ObsClass as oc
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
        try: db_loc = dc( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue
         
        obs.create_station_tables(loc, mode=mode, stage="final")
        db_loc.attach_station_db(loc, output, mode=mode, stage="final")
        
        #sql = ""
        sql_good = (f"INSERT INTO final.obs (dataset,datetime,timestamp,element,value) VALUES "
            f"(?,?,?,?,?) ON CONFLICT DO {on_conflict}")
        sql_bad  = (f"INSERT INTO final.obs_bad (dataset,datetime,element,value,reason) VALUES "
            f"(?,?,?,?,?) ON CONFLICT DO {on_conflict}")
        values_good, values_bad = set(), set()

        for element in elements:
            # element properties and range
            element_info_el                 = element_info[element]
            lower, upper, extra, exclude    = ( element_info_el[i] for i in range(4) )
            #lower                           = int(np.round(float(lower)))
            #upper                           = int(np.round(float(upper)))
            #element_range                   = lower, upper

            # 1 get all 30-min data for this element
            #TODO already check whether value is in range(lower,upper) defined in the element_table
            data = db_loc.exe((f"SELECT dataset,datetime,element,value FROM obs WHERE element "
                f"LIKE '{element}' AND strftime('%M', datetime) IN ('00','30'){and_dataset}"))
            # 2 check for bad (out-of-range) values
            
            for row in data:
                #sql = ""
                
                dataset, datetime, element, val = row[0], dt.fromisoformat(row[1]), row[2], row[3]
                if not dataset: dataset = "unknown" # extra
                
                # if the value does contain the exclude pattern it gets excluded
                if exclude is not None:
                    excluded = re.search(exclude, str(val))
                else: excluded = False

                # if the value is in range OR not numeric this expression will be True
                #try:    val_in_range = ( int(np.round(val)) in element_range )
                try:    val_in_range = (lower <= float(val) <= upper)
                except: val_in_range = True
                
                # for METAR weather code we need to check whether code is correct and fully-known
                if element == "WW_2m_met":
                    # simple approach: we need to check whether there is any correct sub-string
                    #val_in_extra = any(substring in val for substring in extra))
                    
                    # remove leading "-" character if present
                    val = val.lstrip("-")
                    # remove any "-" character if present
                    #val.replace("-", "")
                     
                    # OR we loop through the string in substrings of length 2 and check all values
                    # first we check whether length of string is even (can be read in pieces of 2)
                    lv = len(val)
                    if lv % 2 == 0:
                        # new value string will be constructed by correct sub-strings only
                        #TODO implement as list comprension, at the end join list as str, benchmark
                        val_in_extra    = True
                        val             = ""
                        for i in range(0, lv, 2):
                            if val[i:i+2] in extra:
                                val += val[i:i+2]
                    # else we assume that the whole METAR code is faulty
                    else: val_in_extra = False
                # else we just check whether the value is in list "extra"
                else: val_in_extra = (val in extra)
                
                if not excluded and (val_in_range or val_in_extra):
                    # 3b insert good data into obs table of final database
                    #sql += f"INSERT INTO obs.final (dataset,timestamp,element,value) VALUES ({row[0]},{row[1]},{row[2]},{row[3]})\n"
                    timestamp = int( datetime.timestamp() )
                    values_good.add( (dataset, datetime, timestamp, element, val) )
                else:
                    try:
                        val = float(val)
                        if excluded:
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
                    except Exception as e:
                        # all other exceptions (which should never occur) will be handled seperately
                        reason = f"exception_{e}"
                     
                    # 3b insert bad data into obs_bad table of final database
                    #sql += f"INSERT INTO obs_bad.final (dataset,timestamp,duration,element,value,reason) VALUES ({row[0]},{row[1]},{row[2]},{row[2]},{reason})\n"
                    values_bad.add( (dataset, datetime, element, val, reason) )
        
        db_loc.exemany(sql_good, values_good)
        db_loc.exemany(sql_bad, values_bad)
        #db_loc.exescr(sql)
        
        #TODO implement JUMP CHECK (5th column of element_info dictionary); general procedure idea:
        # if the element changes within {key} hours by {value} drop it from obs database -> obs_bad
        # start iterating through datetimes, starting from i+1 and compare with i-1 and i+2

        # only commit changes when all operations are complete
        db_loc.close(commit=True)

if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Reduce the number of observations according to operation mode"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","u","E")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, clusters=True)
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
    output          = cf.script["output"]
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]
    update          = cf.script["update"]
    extra           = cf.script["extra"]
    sources         = cf.args.source

    if update:
        on_conflict = "UPDATE SET value=excluded.value"
    else:
        on_conflict = "NOTHING"

    element_info    = gf.read_yaml(cf.script["element_info"], file_dir=cf.config_dir)
    elements        = tuple( element_info.keys() )
    
    if len(sources) > 0:
        and_dataset = f" AND dataset {dc.sql_equal_or_in(sources)}"
    else:
        and_dataset = ""

    obs             = oc( cf, mode=mode, stage="forge", verbose=verbose )
    db              = dc( config=cf.database, ro=1 )

    stations        = db.get_stations( clusters )
    #elements        = tuple(f'{element}' for element in db.get_elements())
    
    db.close(commit=False)

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
