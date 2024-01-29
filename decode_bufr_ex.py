#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

import argparse, sys, os, psutil#, shelve
import numpy as np
from glob import glob 
from copy import copy
from itertools import cycle
import eccodes as ec        # bufr decoder by ECMWF
from datetime import datetime as dt, timedelta as td
import global_functions as gf
import global_variables as gv
from database import database_class #as dc
from obs import obs_class #as oc
from bufr import bufr_class as bc
import bufr

#TODO write more (inline) comments, docstrings and make try/except blocks shorter whereever possible
#TODO raises error "API not implemented in CFFI porting"
#see: https://github.com/ecmwf/eccodes-python#experimental-features
#ec.codes_no_fail_on_wrong_length(True)

# check if code already contains replication factor information
no_repl_factor  = lambda code : np.round(code, -3) == code


def get_num_elements_repl_factor(code):
    """
    Parameter:
    ----------
    code: BUFR code with replication factor information

    Notes:
    ------
    Helper function used to interpret specific BUFR codes

    Return:
    -------
    Number of repeated elements and replication factor
    """
    if debug: print("GET NUM ELEMENTS REPL")
    # get number of elements to be repeated
    num_elements = int((code - 100000) / 1000)
    # number of repetitions (1-999) -> replication factor
    repl_factor  = int((code - 100000) - (num_elements * 1000))
    if debug: print(num_elements, repl_factor)
    # return extracted information as tuple
    return num_elements, repl_factor


def get_location_and_datetime(codes, code, vals, val):
    """
    Parameter:
    ----------
    codes: iterator of BUFR codes
    code: current BUFR code
    vals: iterator of values in dataset
    val: current value

    Notes:
    ------
    Helper function which determines location and datetime of current dataset using codes and values

    Return:
    location and datetime, information about how many codes and values to skip in iterator
    """
    #skip_codes, skip_vals   = 4, 0
    skip_codes, skip_vals   = 5, 1
    try:                block = int(val)
    except ValueError:  block = np.nan

    while next(codes) != 1002:
        try:                val = next(vals)
        except ValueError:  val = np.nan
        skip_codes  += 1
        skip_vals   += 1

    try:                station = int(next(vals))
    except ValueError:  station = np.nan

    if not np.isnan(station) and not np.isnan(block):
        location = bufr.to_wmo( block, station )
    else: location = None

    while next(codes) != 4001:
        next(vals)
        skip_codes  +=1
        skip_vals   +=1

    dt_info = []
    for _ in range(5):
        try:                dt_info.append( int(next(vals)) )
        except ValueError:  pass

    try:    datetime = dt(*dt_info)
    except: return None, None, (skip_codes, skip_vals)

    if debug: print(location, datetime, skip_codes, skip_vals)
    return location, datetime, (skip_codes, skip_vals)


def decode_bufr_ex( source=None, file=None, known_stations=None, pid_file=None ):
    #TODO
    """
    Parameter:
    ----------
    source : name of source (str)
    pid_file : name of the file where the process id gets stored (str)

    Notes:
    ------
    main function of the script, parses all files of a given source and tries to save them in database
    using the obs_to_station_db() function. includes file handling and sets the status of a file to
    'locked' before starting to handle it, to 'empty' if no (relevant) data was found in it, to status
    'error' if something went wrong (which should not occur but we never know...) or - if everything
    went smooth to status == 'parsed' in the file_table of the main database. pid_file is optional

    Return:
    -------
    None
    """
    if source:
        config_source   = config_sources[source]
        
        if "bufr" in config_source:
            config_bufr = [config["bufr"], config_script, config_general, config_source["bufr"]]
        else: return

        # previous dict entries will get overwritten by next item during merge (right before left)
        config_bufr = gf.merge_list_of_dicts( config_bufr )
        bf          = bufr.bufr_class(config_bufr, script=script_name[-5:-3])
        bufr_dir    = bf.dir + "/"

        if args.tables:
            os.environ['ECCODES_DEFINITION_PATH'] = args.tables + ":" + tables_default
        elif "tables" in config_bufr:
            os.environ['ECCODES_DEFINITION_PATH'] = config_bufr["tables"] + ":" + tables_default

        try:    clusters = set(config_source["clusters"].split(","))
        except: clusters = None

        db = database_class(config=config_database)

        for i in range(max_retries):
            try:    known_stations = db.get_stations( clusters )
            except: pass
            else:   break

        if i == max_retries - 1:
            sys.exit(f"Can't access main database, tried {max_retries} times. Is it locked?")

        if hasattr(bf, "glob") and bf.glob: ext = f"{bf.glob}.{bf.ext}"
        else:                               ext = f"*.{bf.ext}"
        #TODO add possibility to use multiple extensions (set)

        if args.restart:
            files_to_parse = set(db.get_files_with_status( f"locked_{args.restart}", source ))
        else:
            files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + ext )))

            if args.redo:   skip_files  = set() #db.get_files_with_status( r"locked_%", source )
            else:           skip_files  = db.get_files_with_status( bf.skip_status, source )

            files_to_parse = list( files_in_dir - skip_files )

            #TODO special sort functions for CCA, RRA and stuff in case we dont have sequence key
            #TODO implement order by datetime of files
            if bf.sort_files: files_to_parse = sorted(files_to_parse)
            if bf.max_files:  files_to_parse = files_to_parse[:bf.max_files]

            if verbose:
                print("#FILES IN DIR:  ",   len(files_in_dir))
                print("#FILES TO SKIP: ",   len(skip_files))

        if verbose: print("#FILES TO PARSE:  ",   len(files_to_parse))

        gf.create_dir( bf.dir )

        file_IDs = {}

        for FILE in files_to_parse:

            file_path = gf.get_file_path( bufr_dir + FILE )
            file_date = gf.get_file_date( file_path )

            ID = db.get_file_id(FILE, file_path)
            if not ID:
                status = f"locked_{pid}"
                ID = db.register_file(FILE, file_path, source, status, file_date, verbose=verbose)

            file_IDs[FILE] = ID

        db.close(commit=True)

        #TODO multiprocessing: split files_to_parse by number of processes and parse simultaneously
        # see https://superfastpython.com/restart-a-process-in-python/

    elif file:
        if args.tables:
            os.environ['ECCODES_DEFINITION_PATH'] = args.tables + ":" + tables_default

        FILE            = file.split("/")[-1]
        files_to_parse  = (FILE,)
        file_path       = gf.get_file_path(args.file)
        file_date       = gf.get_file_date(args.file)
        bufr_dir        = "/".join(file.split("/")[:-1]) + "/"
        if args.source: source = args.source
        # default source name for single file: extra
        else:           source = "extra"

        if not known_stations:
            db = database_class(config=config_database)
            known_stations  = db.get_stations()

        ID = db.get_file_id(FILE, file_path)
        if ID: db.set_file_status(ID, "locked")
        else:
            ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)

        db.close(commit=True)

        file_IDs = { FILE : ID }

        config_bufr = gf.merge_list_of_dicts( [config["bufr"], config_script] )
        bf          = bufr.bufr_class(config_bufr, script=script_name[-5:-3])

    #TODO use defaultdic instead?
    obs_bufr, file_statuses, new_obs = {}, set(), 0

    # initialize obs class (used for saving obs into station databases)
    # in this merge we are adding only already present keys; while again overwriting them
    config_obs  = gf.merge_list_of_dicts([config["obs"], config_script], add_keys=False)
    obs         = obs_class(config_obs, source, mode, "raw")

    for FILE in files_to_parse:
        if verbose: print( f"PARSING FILE: {bufr_dir}/{FILE}" ) 
        with open(bufr_dir + FILE, "rb") as f:
            try:
                ID = file_IDs[FILE]
                # if for whatever reason no ID (database lock?) or filestatus means skip:
                # continue with next file
                if not ID: continue
                bufr_file = ec.codes_bufr_new_from_file(f)
                if bufr_file is None:
                    file_statuses.add( ("empty", ID) )
                    if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                ec.codes_set(bufr_file, "unpack", 1)
            except Exception as e:
                log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                if verbose: print(log_str)                
                if traceback: gf.print_trace(e)
                file_statuses.add( ("error", ID) )
                continue
            else: obs_bufr[ID] = {} #shelve.open(f"shelves/{ID}", writeback=True)
            
            if config_script["skip_computed"]:      ec.codes_skip_computed(iterid)
            if config_script["skip_function"]:      ec.codes_skip_function(iterid)
            if config_script["skip_duplicates"]:    ec.codes_skip_duplicates(iterid)

            # get descriptor data (unexpanded descriptors plus their values)
            vals    = tuple( ec.codes_get_array(bufr_file, "numericValues") )
            unexp   = ec.codes_get_array(bufr_file, "unexpandedDescriptors")

            if debug: pdb.set_trace()
             
            codes_exp       = []                    # expanded list of codes / descriptors
            # value and code indices
            pos_val, pos_code = 0, 0

            previous_code = None

            # replace sequence codes by actual sequences #and apply replication factors
            while pos_code < len(unexp):
                code = unexp[pos_code]
                if code in bf.sequence_range and previous_code not in bf.replication_codes:
                    code_offset = 0
                    codes_seq   = bf.bufr_sequences[code]
                    for pos_code_seq, code_seq in enumerate(codes_seq):
                        codes_exp.append(code_seq)
                        pos_val += 1
                else:
                    codes_exp.append(code)
                    pos_val += 1
                pos_code += 1
                previous_code = copy(code)
            
            if debug:
                pdb.set_trace()
                print("VALUES:")
                print( [ i if i != -1e+100 else "" for i in vals ] )
                print("CODES UNEXP:")
                print(list(unexp))
                print("CODES EXP:")
                print(codes_exp)
            
            # replace missing value by proper np.nan
            vals = [ i if i != -1e+100 else np.nan for i in vals ]

            codes       = cycle(codes_exp)  # self-repeating iterator
            vals        = iter(vals)        # iterator over are values (just once)
            obs_list    = []
            
            # station/location and datetime information
            location, datetime = None, None
            previous_code = unexp[0]
        
            def get_repl_codes(codes_repl, code, vals_repl, val):
                """
                Parameter:
                ----------
                codes: iterator of BUFR codes
                code: current BUFR code
                vals: iterator of values in dataset
                val: current value

                Notes:
                ------

                Return:
                -------

                """
                if debug:
                    print("GET REPL")
                    print( bufr.to_code(code), val)
                num_elements, repl_factor   = get_num_elements_repl_factor(code)
                skip_codes, skip_vals       = -1, -1

                if not repl_factor:
                    repl_present = 0
                    #skip_codes += 1
                    #skip_vals += 1
                    """
                    if val < 0:#or np.isnan(val):
                        #skip_vals -= 1
                        next_val    = copy(val)
                    else:
                        #skip_vals   += 1
                        next_val    = next(vals_repl)
                    """
                    next_code   = next(codes_repl)
                    next_val    = next(vals_repl)

                    match next_code:
                        case 31000: # short delayed replication factor
                            if debug: print(next_code, next_val)
                            if next_val and not np.isnan(next_val):
                                repl_factor = 1
                                #skip_vals += 1
                            else: repl_factor = 0
                        case 31001 | 31002: # (extended) delayed replication factor
                            if debug: print(next_code, next_val)
                            #if next_val >= 0 and not np.isnan(next_val):
                            if next_val and not np.isnan(next_val):
                                repl_factor = int(next_val)
                                #skip_vals += 1
                            else: repl_factor = 0
                        case _:
                            if debug: print(code, next_code)
                            ec.codes_release(bufr_file)
                            sys.exit("MISSING REPLICATION FACTOR CODE!")
                else:
                    repl_present = 1
                    skip_codes  -= num_elements + 1

                if not repl_factor: # and not repl_present
                    if debug:
                        print("NO REPL")
                        #print("NEXT elements")
                        #print( [next(vals_repl) for _ in range(num_elements)] )
                    return [], 0, (num_elements + repl_present, 0)
                    #return [], 0, (num_elements + repl_present, repl_present)
                    #return [], 0, (num_elements + repl_present, 1)
                    #TODO incomment for 1 min value stations to work
                    #return [], 0, (num_elements + repl_present, repl_present)
                    #TODO incomment for regular stations to work
                    #return [], 0, (num_elements + 1, 0)
                elif repl_factor == 10:
                    # replication factor 10 is always defined by 3100X code
                    if debug: pdb.set_trace()
                    skip_codes -= 1; skip_vals -= 1

                if num_elements == 1:
                    next_code = next(codes_repl)
                    if next_code in bf.sequence_range:
                        elements = bf.bufr_sequences[next_code]
                        #skip_codes += 1; skip_vals -= 1
                        #skip_vals -= 1
                    else:
                        elements = [next_code]
                        #TODO incomment for 1 min value stations to work
                        #skip_vals -= repl_present
                        #skip_vals -= 1
                else:
                    elements = [ next(codes_repl) for _ in range(num_elements) ]
                    skip_codes -= 1; skip_vals -= 1

                #skip_codes  += num_elements #+ repl_present
                #skip_codes  += repl_present
                skip_vals   += repl_present

                if debug:
                    print("REPL ELEMENTS / REPL / SKIP CODES / SKIP VALS")
                    print(tuple(elements), repl_factor, skip_codes, skip_vals)
                return elements, repl_factor, (skip_codes, skip_vals)

            #continue_loop = True
            #while continue_loop:
            while True:
                try:
                    code = next(codes)
                    if location and datetime:
                        if code == 1001:
                            if debug: print("STOP", location)
                            if obs_list:
                                if debug:
                                    print("OBS LIST:")
                                    print(location, datetime, obs_list)
                                try:    obs_bufr[ID][location][datetime] += obs_list
                                except: obs_bufr[ID][location] = { datetime : obs_list }
                                obs_list = []
                                if debug: pdb.set_trace()
                            val = next(vals)
                            location,datetime,skip  = get_location_and_datetime(codes,code,vals,val)
                            skip_codes, skip_vals   = skip
                            for _ in range(skip_codes): next(codes)
                            for _ in range(skip_vals):  next(vals)

                        elif code in bf.scale_size_alter:
                            if debug: print("SCALE / DATASIZE ALTERATION!")
                            if code in bf.scale_alter:
                                if previous_code != 202129:
                                    obs_list.append( (code, None) )
                                else: del obs_list[-1]
                                previous_code = copy(code)
                        
                        elif code in bf.repl_range:
                            #val = next(vals)
                            codes_repl, repl_factor, skip   = get_repl_codes(codes, code, vals, val)
                            skip_codes, skip_vals           = skip
                            if debug: print("SKIP CODES", skip_codes)
                            for _ in range(skip_codes): next(codes) 
                            if debug: print("SKIP VALUES", skip_vals)
                            for _ in range(skip_vals):  next(vals)

                            if debug: print("REPL:", repl_factor)

                            # aggregate 1 min values to 10 min
                            if repl_factor == 10:
                                print("REPL 10")
                                val_10 = [ next(vals) for _ in range(10) ]
                                print(val_10)
                                if 13011 in codes_repl or 26020 in codes_repl:
                                    print(13011, codes_repl)
                                    obs_list.append( (13011, np.nansum(val_10)) )
                                elif 20003 in codes_repl:
                                    print(20003, codes_repl)
                                    obs_list.append( (20003, np.nanmax(val_10)) )
                            else:
                                for _ in range(repl_factor):
                                    for code_r in codes_repl:
                                        if code_r in bf.scale_size_alter:
                                            print("SCALE / DATASIZE ALTERATION!")
                                            if code in bf.scale_alter:
                                                obs_list.append( (code, None) )
                                            continue
                                        val_r = next(vals)
                                        if debug: print(bufr.to_code(code_r), val_r)
                                        if code_r in bf.relevant_codes and not np.isnan(val_r):
                                            obs_list.append( (code_r, val_r) )
                            
                            #if not repl_factor: next(vals)

                        else:
                            if code in bf.relevant_codes:
                                val = next(vals)
                                if not np.isnan(val):
                                    # turn 1min values to 10min values
                                    if code == 4025 and val == -1: val = -10
                                    if debug: print(bufr.to_code(code), val)
                                    obs_list.append( (code, val) )
                            else:
                                if debug: print(bufr.to_code(code))
                                next(vals)

                    # get blockNumber, stationNumber and datetime info
                    elif code == 1001:
                        val = next(vals)
                        location,datetime,skip  = get_location_and_datetime(codes, code, vals, val)
                        skip_codes, skip_vals   = skip
                        for _ in range(skip_codes): next(codes)
                        for _ in range(skip_vals):  next(vals)
                    
                    else:
                        if debug: print(bufr.to_code(code))
                        next(vals)

                # if we encounter a StopIteration error we break the loop
                except StopIteration:   break
                else:                   continue
                #except StopIteration:   continue_loop = False; continue # break while loop
                #else:                   continue_loop = True            # continue while loop
            
            # end of while loop

            if obs_list:
                if debug:
                    print("OBS LIST:")
                    print(location, datetime, obs_list)
                try:    obs_bufr[ID][location][datetime] += obs_list
                except: obs_bufr[ID][location] = { datetime : obs_list }

        
        # end of with clause (closes file handle)
        ec.codes_release(bufr_file)

        if new_obs:
            file_statuses.add( ("parsed", ID) )
            log.debug(f"PARSED: '{FILE}'")
        else:
            file_statuses.add( ("empty", ID) )
            log.info(f"EMPTY:  '{FILE}'")
        
        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            
            db = database_class(config=config_database)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=bf.timeout)
            db.close()

            print("TOO MUCH RAM USED, RESTARTING...")
            obs_db = bf.convert_keys_ex( obs_bufr, source )
            if obs_db: obs.to_station_databases( obs_db, scale=True )
            
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()
    
    db = database_class(config=config_database)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=bf.timeout)
    db.close()

    if debug: print( obs_bufr )

    obs_db = bf.convert_keys_ex( obs_bufr, source )
    #for ID in obs: obs_bufr[ID].close()

    if obs_db: obs.to_station_databases( obs_db, scale=True, traceback=True )
    
    # remove file containing the pid, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":
  
    msg    = "Decode one or more BUFR files and insert relevant observation data into station databases. "
    msg   += "NOTE: Setting a command line flag or option always overwrites the setting from the config file!"
    parser = argparse.ArgumentParser(description=msg)
    parser.add_argument("-l","--log_level", choices=gv.log_levels, default="NOTSET", help="set log level")
    parser.add_argument("-i","--pid_file", action='store_true', help="create a pid file to check if script is running")
    parser.add_argument("-f","--file", help="parse single file bufr file, will be handled as source=extra by default")
    parser.add_argument("-v","--verbose", action='store_true', help="show detailed output")
    parser.add_argument("-p","--profiler", help="enable profiler of your choice (default: None)")
    #TODO replace profiler by number of processes (prcs) when real multiprocessing (using module) is implemented
    parser.add_argument("-c","--clusters", help="station clusters to consider, comma seperated")
    parser.add_argument("-C","--config", default="config", help="set name of config file")
    parser.add_argument("-d","--dev_mode", action='store_true', help="enable or disable dev mode")
    parser.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    parser.add_argument("-M","--mode", help="set operation mode; options available: {oper, dev, test}")
    parser.add_argument("-n","--max_files", type=int, help="maximum number of files to parse (per source)")
    parser.add_argument("-s","--sort_files", action='store_true', help="sort files alpha-numeric before parsing")
    parser.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    parser.add_argument("-b","--debug", action='store_true', help="enable or disable debugging")
    parser.add_argument("-k","--skip", default="", help="skip [c]omputed, [f]unction and/or [d]uplicate keys")
    parser.add_argument("-e","--extract_values", help="extract additional values (needed for DWD Germany BUFRs)")
    parser.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    parser.add_argument("-T","--tables", help="(absolute) path to ECCODES BUFR tables directory")
    parser.add_argument("-r","--redo", action='store_true', help="decode bufr again even if already processed")
    parser.add_argument("-R","--restart", help=r"only parse all files with status 'locked_{pid}'")
    parser.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")
    #TODO add shelve option to save some RAM
    args = parser.parse_args()

    #read configuration file into dictionary
    config          = gf.read_yaml( args.config )
    script_name     = gf.get_script_name(__file__)
    config_script   = config["scripts"][script_name]
    conda_env       = os.environ['CONDA_DEFAULT_ENV']

    if config_script["conda_env"] != conda_env:
        sys.exit(f"This script needs to run in conda environment {config_script['conda_env']}, exiting!")

    config_general = config["general"]
    tables_default = config["bufr"]["tables"]

    pid = str(os.getpid())

    if args.max_files is not None:  config_script["max_files"]  = args.max_files
    if args.sort_files: config_script["sort_files"] = args.sort_files

    if args.pid_file: config_script["pid_file"] = True
    if config_script["pid_file"]:
        pid_file = script_name + ".pid"
        if gf.already_running( pid_file ):
            sys.exit( f"{script_name} is already running... exiting!" )
    else: pid_file = None

    if args.profiler:
        config_script["profiler"] = args.profiler
    if config_script["profiler"]:
        import importlib
        profiler    = importlib.import_module(config_script["profiler"])
        profile     = True
    else: profile = False

    if args.log_level: config_script["log_level"] = args.log_level
    log = gf.get_logger(script_name)
    
    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"; log.info(started_str)

    if args.verbose is not None: config_script["verbose"] = args.verbose
    verbose = config_script["verbose"]
    if verbose: print(started_str)

    if args.debug:                  config_script["debug"] = True
    if config_script["debug"]:      import pdb; debug = True
    else:                           debug = False

    if args.traceback:              config_script["traceback"] = traceback = True
    else:                           traceback = config_script["traceback"]

    if args.timeout:                config_script["timeout"] = timeout_station = args.timeout
    else:                           timeout_station = config_script["timeout"]

    if args.max_retries:            config_script["max_retries"] = max_retries = args.max_retries
    else:                           max_retries = config_script["max_retries"]

    if args.dev_mode: config_script["mode"] = "dev"
    
    mode = config_script["mode"]
    
    output_path = config["general"]["output_path"]

    # output_path in script config has priority over general config
    if "output_path" in config_script:
        output_path = config_script["output_path"]

    if args.clusters: config_source["clusters"] = frozenset(args.clusters.split(","))

    # get configuration for the initialization of the database class
    config_database = config["database"]

    # add files table (file_table) to main database if not exists
    #TODO this should be done during initial system setup, file_table should be added there
    db = database_class(config=config_database)
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    db.close()

    #parse command line arguments
    if args.file: decode_bufr_ex( file=args.file, pid_file=pid_file )
    elif args.source:
        source = config["sources"][args.source]
        if "," in source:
            sources = source.split(","); config_sources = {}
            for s in sources:
                config_sources[s] = config["sources"][s]
        else: config_sources = { args.source : config["sources"][args.source] }
    else: config_sources = config["sources"]

    if not args.file:
        for SOURCE in config_sources:
            if verbose: print(f"PARSING SOURCE: {SOURCE}")
            decode_bufr_ex( source = SOURCE, pid_file=pid_file )

    stop_time = dt.utcnow()
    finished_str = f"FINISHED {script_name} @ {stop_time}"; log.info(finished_str)
    
    if verbose:
        print(finished_str)
        time_taken = stop_time - start_time
        print(f"{time_taken.seconds}.{time_taken.microseconds} s")
