#!/usr/bin/env python
import os, sys, time, requests, argparse
import global_functions as gf
from config import ConfigClass
from obs import ObsClass
from datetime import datetime as dt, timedelta as td
import logging as log


def convert_imgw_keys(key, data):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """ 
    element     = elements[key][0]
    duration    = elements[key][1]
    value       = elements[key][2] * float(data[key])
    scale       = (10**elements[key][3]) / elements[key][2]
    
    return element, value, duration, scale


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Get thelatest obs from IMGW and insert all relevant data into station databases."
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","c","d","m","o","u")
    cf          = ConfigClass(script_name, flags=flags, info=info, sources=True)
    
    # get currently active conda environment
    conda_env   = os.environ['CONDA_DEFAULT_ENV']

    # check whether script is running in correct environment; if not exit
    if cf.script["conda_env"] != conda_env:
        conda_env = cf.script['conda_env']
        sys.exit(f"This script ({script_name}) needs to run in conda env {conda_env}, exiting!")
    
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
    update          = cf.script["update"]

    if update:
        on_conflict = "UPDATE SET value=excluded.value"
    else:
        on_conflict = "NOTHING"
    
    # get source specific configuration
    config_source   = cf.sources["IMGW"]
    # get priority setting for IMGW source
    prio            = config_source["prio"]
    
    success = False
    for i in range(1, max_retries+1):
        r = requests.get(config_source["url"])
        if r.status_code != 200:
            log.warning(f"Unable to reach IMGW data server, attempt #{i}...")
            time.sleep(timeout)
        else: success = True; break

    if not success:
        error_message = f"Unable to reach IMGW server, tried {max_retries} times | {r.status_code}"
        log.error(error_message)
        sys.exit(error_message)

    obs         = ObsClass(cf, "imgw", stage="raw", verbose=verbose)
    translation = gf.read_yaml("translations/imgw", file_dir=cf.config_dir)
    elements    = translation["elements"]

    meta, obs_db = {}, {}

    for data in r.json():
        for m in translation["meta"]:
            meta[translation["meta"][m]] = data[m]
        
        location = meta["location"] + "0"
        if location not in obs_db: obs_db[location] = set()

        date        = meta["date"]
        hour        = int(meta["hour"])
        datetime    = dt(int(date[:4]), int(date[5:7]), int(date[8:10]), hour)
        
        for key in elements:
            
            # if observation value is None: skip it
            if data[key] is None: continue

            element, value, duration, scale = convert_imgw_keys(key, data)
            
            # precipitation is always only last 6z measurement
            if element == "PRATE_1m_syn":
                datetime_prate = dt(int(date[:4]), int(date[5:7]), int(date[8:10]), 6)
                # if time is earlier than 5z, measurement is from previous day still
                if hour < 6: datetime_prate -= td(days=1)
                obs_db[location].add( ("imgw", datetime_prate, duration, element, value, 0, scale) )
            # for all other elements take usual given datetime 
            else: obs_db[location].add( ("imgw", datetime, duration, element, value, 0, scale) )

    obs.to_station_databases(obs_db, scale=True, prio=prio, update=update)

    finished_str    = gf.get_finished_str(script_name)
    log.info(finished_str)
    if verbose: print(finished_str)

    gf.print_time_taken(start_time)
