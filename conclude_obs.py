#!/usr/bin/env python
import sys
import os
from database import DatabaseClass
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
        gf.create_dir( f"{output_path}/dev/{loc[0]}" )

        sql = [f"ATTACH DATABASE '{output_path}/dev/{loc[0]}/{loc}.db' AS dev"]
        sql.append(f"CREATE TABLE dev.obs AS SELECT DISTINCT datetime,duration,element,value FROM main.obs WHERE element IN{elements} AND strftime('%M', datetime) IN ('00','30')")
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

    cluster         = set( config_script["clusters"].split(",") )
    db              = DatabaseClass( config=config["database"], ro=1 )
    stations        = db.get_stations( cluster )

    elements        = tuple(f'{element}' for element in db.get_elements())
    db.close(commit=False)

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
