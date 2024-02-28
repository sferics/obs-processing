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


def rename_elements(stations):
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
        
        sql = ""

        for old_name, new_name in renamings_dict.items():
            sql += f"UPDATE OR IGNORE obs SET element='{new_name}' WHERE element='{old_name}';\n"
        
        sql = sql[:-2]

        if verbose: print(sql)

        db.exescr(sql)

    return


if __name__ == "__main__":

    # define program info message (--help, -h)
    info        = "Rename old parameter names"
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
    renamings       = cf.script["element_renamings"]

    obs             = ObsClass( config=cf.obs, source=source, mode=mode, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)
    
    renamings_dict  = gf.read_yaml(renamings)

    if processes: # number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=rename_elements(), args=(station_group,))
            p.start()

    else: rename_elements(stations)
