#!/usr/bin/env python
import os, sys, time, requests, argparse
import global_functions as gf
from config import ConfigClass
from obs import ObsClass
from datetime import datetime as dt
import logging as log


def get_obs():
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """ 
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Download latest obs from different Open Data sources. Sources need to be configured in config/sources.yml"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","c","d","m","o","u")
    cf          = ConfigClass(script_name, flags=flags, info=info, sources=True)
    
    # get currently active conda environment
    conda_env   = os.environ['CONDA_DEFAULT_ENV']

    # check whether script is running in correct environment; if not exit
    if cf.script["conda_env"] != conda_env:
        sys.exit(f"This script ({script_name}) needs to run in conda env {cf.script['conda_env']}, exiting!")

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
    
    #TODO iterate sources and download here
    
    finished_str    = gf.get_finished_str(script_name)
    log.info(finished_str)
    if verbose: print(finished_str)

    gf.print_time_taken(start_time)
