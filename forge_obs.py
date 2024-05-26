#!/usr/bin/env python
# THIS IS A CHAIN SCRIPT

import os, sys
import subprocess
import global_variables as gv
import global_functions as gf
from config import ConfigClass
from datetime import datetime as dt
from subprocess import Popen, PIPE

"""
1 reduce_obs.py (only 1 row with max(file) per dataset [UNIQUE datetime,duration,element])
  copy all remaining elements from raw to forge databases [datetime,duration,element,value]
-in forge databases do:
2 audit_obs.py      ->  check each obs, delete bad data like NaN, unknown value or out-of-range
2 derive_obs.py     ->  compute derived elements like RH+TMP=DPT; cloud levels; reduced pressure...
3 aggregate_obs.py  ->  aggregate over time periods (1,3,6,12,24h) and create new elements with _DUR
4 derive_obs.py -A  ->  compute derived elements again, but only 30min values (--aggregated)
5 audit_obs.py      ->  check each obs, delete bad data like NaN, unknown value or out-of-range
                        move good data in file databases e.g. "/oper/final" (oper mode)
                        move bad data to seperate databases, e.g. "/dev/bad" directory (dev mode)
6 empty_obs.py      ->  clear forge databases (they are temporary and get rebuilt every chain cycle)
"""


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Run the complete obs post-processing chain"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","L","d","b","t","e")
    cf          = ConfigClass(script_name, pos=["source"], flags=flags, info=info)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    
    started_str, start_time = gf.get_started_str_time(script_name)
    log.info(started_str)
    
    # define some shorthands from script config
    verbose         = cf.script["verbose"]
    legacy_output   = cf.script["legacy_output"]
    export          = cf.script["export"] 
    mode            = cf.script["mode"]

    match mode:
        case "oper":
            scripts = ["reduce", "derive", "aggregate", "derive", "audit", "empty"]
        case "dev":
            scripts = ["reduce", "aggregate", "derive", "audit", "empty"]
        case "test":
            raise NotImplementedError("TODO: TEST MODE")
        case _:
            raise ValueError("UNSUPPORTED MODE")

    if export: scripts.append("export")
    
    # get all provided command line arguments as a list
    cli_args = sys.argv[1:]
    
    if export and legacy_output:
        # returns "-L" or "--legacy_output" if either of them are found in cli arguments; else None
        arg_L = gf.values_in_list(("-L", "--legacy_output"), cli_args)

        # if legacy_output: remove temporarly from argument list
        # we will only add it later to the export_obs script
        if arg_L:
            pos_L = cli_args.index(arg_L)
            del cli_args[pos_L:pos_L+1]
        
        # also temporarily delete the -e / --export flag if present
        for flag in ("-e","--export"):
            try:    cli_args.remove(flag)
            except: pass
            else:   break

    aggregated = 0

    # https://stackoverflow.com/questions/8953119/waiting-for-external-launched-process-finish

    for script in scripts:
        # if export is set True we define the -L flag only for the export_obs script
        if script == "export" and export and legacy_output:
            cli_args += ["-L", legacy_output]
        elif script == "aggregate":
            aggregated += 1
        # if derive_obs.py is called after aggregate_obs.py we need to add the -A flag
        elif script == "derive" and aggregated:
            cli_args.append("-A")
        elif "-A" in cli_args:
            cli_args.remove("-A")
        if cf.args.bare:
            print("python", script+"_obs.py", *cli_args)
        else:
            try:
                #os.execl( "python", script+"_obs.py", *cli_args )
                print(["python", script+"_obs.py"] + cli_args)
                process = Popen(["python", script+"_obs.py"] + cli_args, stdout=PIPE, stderr=PIPE)
                process.wait()
                stdout, stderr = process.communicate()
                print(stdout)
            except Exception as e:
                gf.print_trace(e)
                sys.exit( f"FAILED TO RUN '{script}_obs.py'! STOPPING CHAIN..." )

    finished_str    = gf.get_finished_str(script_name) 
    log.info(finished_str)
    if verbose: print(finished_str)
    
    gf.print_time_taken(start_time)
