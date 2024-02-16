#!/usr/bin/env python
# cleaning obs database in raw,live and dev (argument)
# gets called in different frequencies for these options
# e.g.:
# raw:      once a month (or year?)
# live:     once a week
# dev:      never?

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

    obs             = ObsClass( typ="forge", mode=mode, config=config_script, source="test" )
    cluster         = set( config_script["clusters"].split(",") )
    db              = DatabaseClass( config=config["database"], ro=1 )
    stations        = db.get_stations( cluster ); db.close(commit=False)
