#!python
import os, sys, time, requests, argparse
import global_functions as gf
from obs import obs
from datetime import datetime as dt
import logging as log

def convert_imgw_keys(key, data):
    element     = elements[key][0]
    duration    = elements[key][1]
    value       = elements[key][2] * float(data[key])
    return element, value, duration

if __name__ == "__main__":
    config          = gf.read_yaml("config.yaml")
    config_script   = config["scripts"][sys.argv[0]]
    conda_env       = os.environ['CONDA_DEFAULT_ENV']
    if config_script["conda_env"] != conda_env:
        sys.exit(f"This script needs to run in conda environment {config_script['conda_env']}, exiting!")

    OBS = obs("imgw", config["obs"])

    config_source   = config["sources"]["IMGW"]
    config_general  = config["general"]
    output_oper     = config_general["output_oper"]
    output_dev      = config_general["output_dev"]

    msg    = "Get latest obs from polish weather service and insert observatio data into database."
    parser = argparse.ArgumentParser(description=msg)

    # add arguments to the parser
    log_levels = { "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG" }
    parser.add_argument("-l","--log_level", choices=log_levels, default="NOTSET", help="set log level") 
    parser.add_argument("-v","--verbose", action='store_true', help="show detailed output")
    parser.add_argument("-c","--config", default="config.yaml", help="set name of yaml config file")
    parser.add_argument("-d","--dev_mode", action='store_true', help="enable or disable dev mode")
    parser.add_argument("-m","--max_retries", help="maximum attemps when communicating with IMGW server")
    parser.add_argument("-o","--timeout", help="waiting timeout (in seconds) for HTTPS connection")
    args = parser.parse_args()

    if args.log_level: config_script["log_level"] = args.log_level
    log.basicConfig(filename=f"{sys.argv[0]}.log", filemode="w", level=eval(f"log.{config_script['log_level']}"))

    started_str = f"STARTED {sys.argv[0]} @ {dt.utcnow()}"; log.info(started_str)

    if args.verbose:        verbose = True
    else:                   verbose = config_script["verbose"]
    if verbose: print(started_str)
    
    if args.timeout:        timeout     = args.timeout
    else:                   timeout     = config_script["timeout"]
    if args.max_retries:    max_retries = args.max_retries
    else:                   max_retries = config_script["max_retries"]

    success = False
    for i in range(1,max_retries+1):
        r = requests.get(config_source["url"])
        if r.status_code != 200:
            log.warning(f"Unable to reach IMGW data server, attempt #{i}...")
            time.sleep(timeout)
        else: success = True; break

    if not success:
        error_message = f"Unable to reach IMGW data server, tried {max_retries} times!"
        log.error(error_message)
        sys.exit(error_message)

    timeout_db  = config["database"]["timeout"]
    db_file     = config["database"]["db_file"]

    if args.dev_mode:                   config_script["dev_mode"] = True
    elif config_general["dev_mode"]:    config_script["dev_mode"] = True
    if config_script["dev_mode"]:       output_path = config_general["output_dev"]
    else:                               output_path = config_general["output_oper"]
    gf.create_dir(output_path)

    translation = gf.read_yaml("imgw_translation.yaml")
    elements    = translation["elements"]

    config_station_dbs = config_general["station_dbs"]
    timeout_stations = config_station_dbs["timeout"]
    retries_stations = config_station_dbs["max_retries"]

    meta, obs = {}, {}

    for data in r.json():
        for m in translation["meta"]:
            meta[translation["meta"][m]] = data[m]
        
        location = meta["location"] + "0"
        if location not in obs: obs[location] = set()

        date = meta["date"]; hour = int(meta["hour"])
        datetime = dt(int(date[:4]), int(date[5:7]), int(date[8:10]), hour)

        for key in elements:
            # if observation value is None: skip it
            if data[key] is None: continue
            element, value, duration = convert_imgw_keys(key, data)
            obs[location].add( (0, datetime, duration, element, value, 0) )

    OBS.to_station_databases(obs)

    finished_str = f"FINISHED {sys.argv[0]} @ {dt.utcnow()}"; log.info(finished_str)
    if verbose: print(finished_str)
