#!/usr/bin/env python
# THIS IS A CHAIN SCRIPT

import os, sys
import subprocess
import global_variables as gv
import global_functions as gf
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
    
    from config import ConfigClass as cc

    #cf = cc()

    import argparse

    # define program info message (--help, -h) and parser arguments with explanations on them (help)
    info    = "Run the complete obs post-processing chain"
    psr     = argparse.ArgumentParser(description=info)

    # add all needed command line arguments to the program's interface
    psr.add_argument("-l","--log_level", choices=gv.log_levels, default="NOTSET", help="set logging level")
    psr.add_argument("-v","--verbose", action="store_true", help="show more detailed output")
    psr.add_argument("-C","--config", default="config", help="set custom name of config file")
    psr.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    psr.add_argument("-M","--mode", choices={"oper", "dev", "test"}, help="set operation mode")
    psr.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    psr.add_argument("-O","--output", help="define output directory where the station databases will be saved")
    psr.add_argument("-L","--legacy_output", help="define output directory for old system's CSV format")
    psr.add_argument("-d","--debug", action="store_true", help="enable or disable debugging")
    psr.add_argument("-D","--dry", action="store_true", help="do not actually run scripts, only print commands")
    psr.add_argument("-t","--traceback", action="store_true", help="enable or disable traceback")
    psr.add_argument("-e","--export", action="store_true", help="export data to legacy CSV format")
    psr.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")

    # parse all command line arguments and make them accessible via the args variable
    args = psr.parse_args()

    # read configuration file into dictionary
    config          = gf.read_yaml( args.config )

    # get the right script name
    script_name     = gf.get_script_name(__file__)
    config_script   = config["scripts"][script_name]

    if args.log_level: config_script["log_level"] = args.log_level
    log_level = config_script["log_level"]
    log = gf.get_logger(script_name)

    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"
    log.info(started_str)

    if args.verbose is not None:
        config_script["verbose"] = args.verbose

    verbose = int(config_script["verbose"])
    if verbose: print(started_str)

    if args.debug:                  config_script["debug"]          = 1
    if config_script["debug"]:      import pdb; debug = 1
    else:                           debug = 0

    if args.traceback:              config_script["traceback"]      = 1
    traceback = config_script["timeout"]

    if args.timeout:                config_script["timeout"]        = int(args.timeout)
    timeout = config_script["timeout"]

    if args.max_retries:            config_script["max_retries"]    = int(args.max_retries)
    max_retries = config_script["max_retries"]

    if args.mode:                   config_script["mode"]           = args.mode
    mode = config_script["mode"]

    match mode:
        case "oper":
            scripts = ["reduce", "audit", "derive", "aggregate", "conclude"]
        case "dev":
            scripts = ["reduce", "derive", "aggregate", "conclude"]
        case "test":
            raise NotImplementedError("TODO: TEST MODE")
        case _:
            raise ValueError("UNSUPPORTED MODE")

    if args.output:                 config_script["output"]         = args.output
    output = config_script["output"]

    if args.export:                 config_script["export"]         = int(args.export)
    export = config_script["export"]

    if args.legacy_output:
        config_script["legacy_output"]  = args.legacy_output
        export                          = 1
    legacy_output = config_script["legacy_output"]

    if export: scripts.append("export")
    
    # get all provided command line arguments as a list
    cli_args    = sys.argv

    # returns "-L" or "--legacy_output" if either of them are found in the cli arguments; else None
    arg_L       = gf.values_in_list(("-L", "--legacy_output"), cli_args)

    # if legacy output: remove temporarly from argument list
    # we will only add it later to the export_obs script
    if arg_L:
        pos_L = cli_args.index(arg_L)
        del cli_args[pos_L:pos_L+1]

    # https://stackoverflow.com/questions/8953119/waiting-for-external-launched-process-finish

    for script in scripts:
        # if export is set True we set the -L flag only for the export_obs script
        if script == "export" and export and legacy_output:
            cli_args += ["-L", legacy_output]
        if args.dry:
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
