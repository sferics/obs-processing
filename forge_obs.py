#!/usr/bin/env python
# THIS IS A CHAIN SCRIPT

import os, sys
import subprocess
import global_variables as gv
import global_functions as gf
from config import ConfigClass
from datetime import datetime as dt
from subprocess import Popen, PIPE

# 1 reduce_obs.py (only 1 row with max(file) per dataset [UNIQUE datetime,duration,element])
#   copy all remaining elements from raw to forge databases [datetime,duration,element,value]
# -in forge databases do:
# 2 audit_obs.py        -> check each obs, delete bad data like NaN, unknown value or out-of-range
#   OR instead(?): move bad data to seperate databases, e.g. in "/dev/oper/bad" directory (dev mode)
# 3 derive_obs.py       -> compute derived elements like RH+T=TD; cloud levels; reduced pressure...
# 4 aggregate_obs.py    -> aggregate over time periods (1,3,6,12,24h) (and create new elements???)
# 5 conclude_obs.py (alias/symlink finalize_obs.py or some other better fitting name???)
#   copy all relevant obs elements (main database element_table) from forge to dev or oper database,
#   depending on operation mode; if this action is complete, maybe do some last checks? afterwards:
#   clear all forge databases (they are just temporary and will be rebuilt in every chain cycle)


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Run the complete obs post-processing chain"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","L","d","D","t","e")
    cf          = ConfigClass(script_name, pos=["source"], flags=flags, info=info, verbose=True)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"
    
    log.info(started_str)
    
    # define some shorthands from script config
    verbose         = cf.script["verbose"]
    legacy_output   = cf.script["legacy_output"]
    export          = cf.script["export"] 
    mode            = cf.script["mode"]

    match mode:
        case "oper":
            scripts = ["reduce", "audit", "derive", "aggregate", "conclude"]
        case "dev":
            scripts = ["reduce", "derive", "aggregate", "conclude"]
        case "test":
            raise NotImplementedError("TODO: TEST MODE")
        case _:
            raise ValueError("UNSUPPORTED MODE")

    if export: scripts.append("export")
    
    # get all provided command line arguments as a list
    cli_args    = sys.argv
    
    if export and legacy_output:
        # returns "-L" or "--legacy_output" if either of them are found in cli arguments; else None
        arg_L       = gf.values_in_list(("-L", "--legacy_output"), cli_args)

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

    # https://stackoverflow.com/questions/8953119/waiting-for-external-launched-process-finish

    for script in scripts:
        # if export is set True we define the -L flag only for the export_obs script
        if script == "export" and export and legacy_output:
            cli_args += ["-L", legacy_output]
        if cf.args.dry:
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

    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)

    if verbose: print(finished_str)

    time_taken = stop_time - start_time
    print(f"{time_taken.seconds}.{time_taken.microseconds} s")
