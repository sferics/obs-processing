#!/usr/bin/env python
# cleaning obs database in raw,live and dev (argument)
# gets called in different frequencies for these options
# e.g.:
# raw:      once a month (or year?)
# live:     once a week
# dev:      never?

from config import ConfigClass
from obs import ObsClass
from database import DatabaseClass

if __name__ == "__main__":

    # define program info message (--help, -h)
    info        = "Cleanup old observations according to operation mode"
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
