#!/usr/bin/env python
import os
#import numpy as np
from operator import itemgetter
from itertools import groupby
from collections import defaultdict
import global_functions as gf
import global_variables as gv
import sql_factories as sf
from database import DatabaseClass


def delete_duplicate_obs(stations):
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
    
    import argparse

    # define program info message (--help, -h) and parser arguments with explanations on them (help)
    info    = "Run the complete obs post-processing chain"
    psr     = argparse.ArgumentParser(description=info)

    # add all needed command line arguments to the program's interface
    psr.add_argument("-l","--log_level", choices=gv.log_levels, default="NOTSET", help="set logging level")
    psr.add_argument("-v","--verbose", action='store_true', help="show more detailed output")
    psr.add_argument("-C","--config", default="config", help="set custom name of config file")
    psr.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    psr.add_argument("-M","--mode", help="set operation mode; options available: {oper, dev, test}")
    psr.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    psr.add_argument("-O","--output", help="define output directory where the station databases will be saved")
    psr.add_argument("-d","--debug", action='store_true', help="enable or disable debugging")
    psr.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    psr.add_argument("-e","--export", action="store_tru", help="export data to legacy CSV format")
    psr.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")

    # parse all command line arguments and make them accessible via the args variable
    args = psr.parse_args()

    # if source argument is provided set source info accordingly
    if args.source: source = args.source
    # default source name is test
    #TODO if no source is provided it should instead iterate over all sources, like in decode_bufr.py
    else:           source = "test"

    config          = gf.read_yaml( "config" )
    db_settings     = config["database"]["settings"]
    script_name     = gf.get_script_name(__file__)
    config_script   = config["scripts"][script_name]
    output_path     = config_script["output_path"]
    verbose         = config_script["verbose"]
    traceback       = config_script["traceback"]
    mode            = config["general"]["mode"]

    if "mode" in config_script:
        mode = config_script["mode"]

    db              = DatabaseClass( config=config["database"] )

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
