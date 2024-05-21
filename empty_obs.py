#!/usr/bin/env python
from config import ConfigClass as cc
from database import DatabaseClass as dc
from obs import ObsClass as oc


def empty_obs( stations, mode, stage ):
    
    for loc in stations:
        
        db_loc_path = oc.get_station_db_path(loc, output, mode, stage) 
        db_loc      = dc(db_loc_path)
        sql         = "DELETE from obs"
        
        if mode == "final" and bad_obs:
            sql += ";\nDELETE from obs_bad"

        db_loc.exescr(sql)
        db_loc.close(commit=True)
    
    return


if __name__ == "__main__":

    # define program info message (--help, -h)
    info        = "Empty all observations according to operation mode and stage"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","S","B")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, verbose=True)
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
    stage           = cf.script["stage"]
    output          = cf.script["output"] + "/" + mode
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]
    bad_obs         = cf.script["bad_obs"]

    db              = dc( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)
    
    empty_obs(stations, mode, stage)
