#!/usr/bin/env python
# decodes BUFRs for availabe or given sources and saves obs to database

# system modules
import argparse, sys, os, psutil#, shelve
import numpy as np
from glob import glob 
from copy import copy
from itertools import cycle
#import ecmwflibs
# bufr decoder by ECMWF
import eccodes as ec
from datetime import datetime as dt, timedelta as td
import global_functions as gf
import global_variables as gv
from database import DatabaseClass as dc
from obs import ObsClass as oc
from bufr import BufrClass as bc

#TODO write more (inline) comments, docstrings and make try/except blocks shorter whereever possible
#TODO raises error "API not implemented in CFFI porting"
#see: https://github.com/ecmwf/eccodes-python#experimental-features
#ec.codes_no_fail_on_wrong_length(True)

# check if code already contains replication factor information
# TODO can be removed?
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
    # get the WMO block number which is the current value, np.nan if no numeric value
    try:                block = int(val)
    except ValueError:  block = np.nan

    if debug: print("BLOCK", block)

    # wait for the WMO station number key to appear
    while next(codes) != 1002:
        val = next(vals)
        skip_codes  += 1
        skip_vals   += 1

    # get the station number value by iterating vals once, np.nan if no numeric value
    try:                station = int(next(vals))
    except ValueError:  station = np.nan
    
    if debug: print("STATION", station)

    # both station and block number should be numeric values (integers)
    if not np.isnan(station) and not np.isnan(block):
        # from block and station number, deviate the WMO location number
        location = bc.to_wmo( block, station )
    # otherwise, we do not have a valid WMO location
    else: return "", "", (0, 0)

    print("WMO", location)

    # wait for the 'year' BUFR key to appear in the cycle iterator
    while next(codes) != 4001:
        if debug:   print(next(vals))
        else:       next(vals)
        skip_codes  +=1
        skip_vals   +=1

    # get datetime info by iterating over the next 5 consecutive values
    dt_info = []
    for _ in range(5):
        next_val = next(vals)
        print(_, next_val)
        try:                dt_info.append( int(next_val) )
        #try:                dt_info.append( int(next(vals)) )
        # if this fails we need to skip yet one for value
        except ValueError:  return "", "", (0, 0)
    
    # finally, generate a datetime object from the dt_info list
    try:    datetime = dt(*dt_info)
    # if no valid datetime object can be created: return None for location and datetime
    except: return "", "", (0, 0)

    if debug: print(location, datetime, skip_codes, skip_vals)
    # return location, datetime and tuple with skip information
    return location, datetime, (skip_codes, skip_vals)


def decode_bufr_ex( source=None, file=None, known_stations=None, pid_file=None ):
    """
    Parameter:
    ----------
    source : name of source (optional)
    file : name of single file to process (optional)
    known_stations: list of known stations to consider (optional)
    pid_file: name of the pid file which stores the process id (optional)

    Notes:
    ------
    main function of the script, parses all files of a given source and saves them into database
    using the obs.to_station_databases function. includes file handling and sets status of a file to
    'locked' before starting to handle it, to 'empty' if it did not contain any (relevant) data, to
    'error' if something went wrong (which should not occur but we never know...) OR - if everything
    went smooth to status == 'parsed' in the file_table of the main database. pid_file is optional

    Return:
    -------
    implicit None
    """
    if source:
        # get source-specific settings by subscripting the config_source dict with the source name
        config_source = config_sources[source]
        
        if "bufr" in config_source:
            # list of all configs in reverse order of significance (right has priority over left)
            config_list = [ config["bufr"], config_script, config_general, config_source["bufr"] ]
        else: return

        # previous dict entries will get overwritten by next item during merge (right before left)
        config_bufr = gf.merge_list_of_dicts( config_list )
        # create bufr class object which contains config, functions and many variables we need
        bf          = bc(config_bufr, script=script_name[-5:-3])
        bufr_dir    = bf.dir + "/"

        # in case we need BUFR tables, set the necessary environment variables for ECCODES
        # info on env vars: https://stackoverflow.com/questions/8365394/set-environment-variable-in-python-script
        if args.tables or "tables" in config_bufr:
            env_var = "ECMWFLIBS_ECCODES_DEFINITION_PATH"
            if args.tables:
                os.environ[env_var] = args.tables + ":" + tables_default
            else:
                os.environ[env_var] = config_bufr["tables"] + ":" + tables_default
            print(f"DEFINITION_PATH: {os.environ[env_var]}")
            #eccodes_path = ecmwflibs.find("eccodes")
            
        # get cluster information from config which is source-specific; therefore happens here
        try:    clusters = config_source["clusters"]
        # if clusters setting is not present or in case of any errors, process all clusters
        except: clusters = None
        
        # create database object
        db = dc(config=config_database)
        
        # try to access database to retrieve list of known stations; use max_retries settings here
        for i in range(max_retries):
            try:    known_stations = db.get_stations( clusters )
            # if it fails try again
            except: pass
            # else exit the loop
            else:   break
        
        # when the maximum number of retries has been reached; quit the program if still no success
        if i == max_retries - 1:
            sys.exit(f"Can't access main database, tried {max_retries} times. Is it locked?")
        
        # if we want to select file names according to glob patterns, use globbing extensions
        if hasattr(bf, "glob") and bf.glob: ext = f"{bf.glob}.{bf.ext}"
        else:                               ext = f"*.{bf.ext}"
        #TODO add possibility to use multiple extensions (set)

        if args.restart:
            # if a restart has been triggered (due to full RAM) just process the locked_ files
            files_to_parse = set(db.get_files_with_status( f"locked_{args.restart}", source ))
        else:
            # otherwise get all filenames in directory as a set
            files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + ext )))

            # if we want to reprocess files; skip_files will only consist of locked_ files
            if args.redo:   skip_files  = db.get_files_with_status( r"locked_%", source )
            # otherwise get all files with statuses that we want to skip (defined in db class)
            else:           skip_files  = db.get_files_with_status( bf.skip_status, source )
            
            # files_parse is a list of all files in directory minus the ones we want to skip
            files_to_parse = list( files_in_dir - skip_files )

            #TODO special sort functions for CCA, RRA and stuff in case we dont have sequence key
            #TODO implement order by datetime of files
            # we sort the files by name which should result in an order by date/time (in most cases)
            if bf.sort_files: files_to_parse = sorted(files_to_parse)
            # if max_files is defined we only process a specific number of files
            if bf.max_files:  files_to_parse = files_to_parse[:bf.max_files]

            if verbose:
                print("#FILES IN DIR:  ",   len(files_in_dir))
                print("#FILES TO SKIP: ",   len(skip_files))

        if verbose: print("#FILES TO PARSE:  ",   len(files_to_parse))

        # create the station files output directory if it does not exist yet
        gf.create_dir( bf.dir )
        
        # unique file IDs (rowids of the file_table) will be stored in a dictionary
        file_IDs = {}

        # loop over all files which we want to parse
        for FILE in files_to_parse:

            # get the full file path by combining bufr directory and file name
            file_path = gf.get_file_path( bufr_dir + FILE )
            # get the file date from the file path (see function in global_functions for details)
            file_date = gf.get_file_date( file_path )
            
            # get the unique file ID from the main databases file_table (by simply using rowid)
            ID = db.get_file_id(FILE, file_path)
            if not ID:
                # if there is no ID yet register it and set its status to locked_ + its process ID
                status = f"locked_{pid}"
                ID = db.register_file(FILE, file_path, source, status, file_date, verbose=verbose)
            
            # save file ID to dict
            file_IDs[FILE] = ID

        # close database connection WITH committing afterwards
        db.close(commit=True)

        #TODO multiprocessing: split files_to_parse by number of processes and parse simultaneously
        # see https://superfastpython.com/restart-a-process-in-python/
    
    # processing single file only
    elif file:
        # once again if we need special BUFR tables provide an environment variable for ECCODES
        if args.tables:
            env_var = "ECMWFLIBS_ECCODES_DEFINITION_PATH"
            os.environ[env_var] = args.tables + ":" + tables_default
            print(f"DEFINITION_PATH: {os.environ[env_var]}")
            #eccodes_path = ecmwflibs.find("eccodes")

        # get only the file name itself, without path
        FILE            = file.split("/")[-1]
        # only one file to parse, so tuple of length 1
        files_to_parse  = (FILE,)
        # get file path of the single file
        file_path       = gf.get_file_path(args.file)
        # get its creation date
        file_date       = gf.get_file_date(args.file)
        # name of the directory where the file is from
        bufr_dir        = "/".join(file.split("/")[:-1]) + "/"
        # if source argument is provided set source info accordingly
        if args.source: source = args.source
        # default source name for single file: extra
        else:           source = "extra"

        # do not consider known_stations; instead save data from all stations
        if not known_stations:
            # new database connection
            db = dc(config=config_database)
            # list of known station from database (station_table)
            known_stations  = db.get_stations()

        # get ID of file if present
        ID = db.get_file_id(FILE, file_path)
        # if ID returned set its status to 'locked'
        if ID:  db.set_file_status(ID, "locked")
        # else register new file with status 'locked' and retrieve its ID
        else:   ID = db.register_file( FILE, file_path, source, "locked", file_date, verbose )
        
        # close database connection again while force-committing all changes
        db.close(commit=True)
        
        # the dictionary of file_IDs to process has now only one key/element
        file_IDs = { FILE : ID }
        
        # for just 2 configuration dicts we can use the easier, more pythonic syntax
        config_bufr = config["bufr"] | config_script
        #config_bufr = gf.merge_list_of_dicts( [config["bufr"], config_script] )
        bf          = bc(config_bufr, script=script_name[-5:-3])

    #TODO use defaultdic instead?
    obs_bufr, file_statuses, new_obs = {}, set(), 0

    # initialize obs class (used for saving obs into station databases)
    # in this merge we are adding only already present keys; while again overwriting them
    config_obs  = gf.merge_list_of_dicts( [config["obs"], config_script], add_keys=False )
    # create obs object which will be used to save observation into the database
    obs         = oc(config_obs, source, mode, "raw")
    
    # iterate over list of all files we want to parse
    for FILE in files_to_parse:
        if verbose: print( f"PARSING FILE: {bufr_dir+FILE}" )
        # open file savely as readonly (byte-mode)
        with open(bufr_dir + FILE, "rb") as file_handle:
            try:
                # the file ID is the representation of the file in the main database (file_table)
                ID = file_IDs[FILE]
                # if for whatever reason no ID (database lock?) or filestatus means skip:
                # continue with next file
                if not ID: continue
                # let ECCODES read the file into memory and handle it
                msg = ec.codes_bufr_new_from_file(file_handle)
                # msg should be an integer (ID); if not it is None (empty) and needs to be skipped
                if msg is None:
                    # status is empty if the bufr file contains no data
                    file_statuses.add( ("empty", ID) )
                    if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                # skip extra attributes like units and scale to decode 25% faster (we can get them via key->code)
                # https://confluence.ecmwf.int/display/UDOC/Performance+improvement+by+skipping+some+keys+-+ecCodes+BUFR+FAQ)
                ec.codes_set(msg, 'skipExtraKeyAttributes', 1)
                # tell ECCODES to unpack all BUFR data, keys and elements
                ec.codes_set(msg, "unpack", 1)
                # extract all subsets
                ec.codes_set(msg, "extractSubsetIntervalStart", 1)
                ec.codes_set(msg, "extractSubsetIntervalEnd", ec.codes_get_long(msg, "numberOfSubsets"))
                ec.codes_set(msg, "doExtractSubsets", 1)
            # if anything goes wrong we log an error message and declare the file status as 'error'
            except Exception as e:
                log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                if verbose: print(log_str)
                if traceback: gf.print_trace(e)
                # set file status to 'error' in order to be able to track down what went wrong
                file_statuses.add( ("error", ID) )
                continue
            # if all went down smoothly create an empty dictionary to store the data in it
            else: obs_bufr[ID] = {} #shelve.open(f"shelves/{ID}", writeback=True)

            # get descriptor data (unexpanded descriptors plus their values)
            vals    = ec.codes_get_double_array(msg, "numericValues")
            unexp   = ec.codes_get_long_array(msg, "unexpandedDescriptors")

            if debug:
                print(obs_bufr)
                pdb.set_trace()
             
            codes_exp = [] # expanded list of codes / descriptors
            # value and code indices
            pos_val, pos_code = 0, 0
            # previous code is None at first and will be needed to spot repeated sequences
            previous_code = None
            
            # as long as we did not reach the and of the unexpanded data array; continue
            while pos_code < len(unexp):
                # get code by subscripting position in the unexp array
                code = unexp[pos_code]
                # if the code is a sequence but no replication info sits directly in front of it
                if code in bf.sequence_range and previous_code not in bf.repl_info:
                    # replace sequence codes by actual sequences and apply replication factors
                    codes_seq = bf.bufr_sequences[code]
                    # add code sequence to list of expanded descriptors
                    codes_exp += codes_seq
                # else just add the code to the list
                else: codes_exp.append(code)
                # count up the position indicator
                pos_code += 1
                # remember the previous code to check whether it contains replication info
                previous_code = copy(code)
            
            if debug:
                pdb.set_trace()
                print("VALUES:")
                print( [ i if i != -1e+100 else "" for i in vals ] )
                print("CODES UNEXP:")
                print(list(unexp))
                print("CODES EXP:")
                print(codes_exp)
            
            # replace missing values by proper 'np.nan' so we can easily detect and exclude NaNs
            vals = [ i if i != -1e+100 else np.nan for i in vals ]
            
            # create a self-repeating iterator which will always start from top again (first code)
            codes       = cycle(codes_exp)
            # the value iterator will be exhausted after it hit the last element (used for values)
            vals        = iter(vals)
            # list of observation contained in the bufr file
            obs_list    = []
            
            # station/location and datetime information
            location, datetime = None, None
            # we need to keep track of the previous code; 1st code in the beginning of the iteration
            previous_code = unexp[0]
    
            #TODO instead of defining the following functions and using standard iterators we could also write
            #our own (complex) generator function with explicit return and/or yield from statements or decorators
            #inspiration here: https://pub.aimind.so/subtle-aspects-of-pythons-generator-function-3a244e197da

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
                elements, repl_factor, (skip_codes, skip_vals)
                """
                if debug:
                    print("GET REPL")
                    print( bc.to_code(code), val)
                # get number of element to be repeated and the amount of repetitions (repl_factor)
                num_elements, repl_factor   = get_num_elements_repl_factor(code)
                # initial values of skip_codes and skip_values; define how many codes/values to skip
                # this happens in while loop after this function with main vals and codes iterators
                skip_codes, skip_vals       = -1, -1

                # repl_factor is the replication factor, ergo how many times a code gets repeated
                if not repl_factor:
                    # repl_present tells whether the code already contained replication info
                    repl_present    = 0
                    # get the next iterations of the local, function-internal iterators
                    #TODO maybe it is yet better to use only the global ones? avoiding confusion...
                    next_code       = next(codes_repl)
                    next_val        = next(vals_repl)
                    
                    # check the next_code, whether it provides a (short/extended) replication factor
                    match next_code:
                        case 31000: # short delayed replication factor
                            if debug: print(next_code, next_val)
                            # if the next value is not zero or contains a NaN set repl_factor to 1
                            if next_val and not np.isnan(next_val): repl_factor = 1
                            else:                                   repl_factor = 0
                        case 31001 | 31002: # (extended) delayed replication factor
                            if debug: print(next_code, next_val)
                            # if the next value is not zero or NaN: set repl_factor to value itself
                            if next_val and not np.isnan(next_val): repl_factor = int(next_val)
                            else:                                   repl_factor = 0
                        case _: # this case should rarely occur and actually only in case of faulty BUFR data
                            # in case we get some other code; release BUFR and skip this file to be save
                            ec.codes_release(msg)
                            # log an error message with the file that is currently processed + 'code/next_code'
                            log_str = f"ERROR:  '{FILE}' (MISSING REPLICATION FACTOR CODE '{code}/{next_code}')"
                            log.error(log_str)
                            if verbose or debug: print(log_str)
                            # set file status to 'error' in order to be able to track down what went wrong
                            file_statuses.add( ("error", ID) )
                            # raising a ValueError enables us to exit the function and except the error outwards
                            raise ValueError
                else:
                    repl_present = 1
                    skip_codes  -= num_elements + 1

                if not repl_factor: # and not repl_present
                    if debug: print("NO REPL")
                    # if no element to be repeated: return an empty list and 0 as replication factor
                    return [], 0, (num_elements + repl_present, 0)
                elif repl_factor == 10:
                    # replication factor 10 is always defined by 3100X code
                    if debug: pdb.set_trace()
                    skip_codes -= 1; skip_vals -= 1

                if num_elements == 1:
                    next_code = next(codes_repl)
                    # if next_code is a BUFR sequence get the elements it contains
                    if next_code in bf.sequence_range:
                        elements = bf.bufr_sequences[next_code]
                    else:
                        # otherwise elements is just a single-element list
                        elements = [next_code]
                else:
                    # get the next elements in range and save them into a list comprehension
                    elements = [ next(codes_repl) for _ in range(num_elements) ]
                    skip_codes -= 1; skip_vals -= 1

                # only if a replication factor is given directly in the code: skip one value
                skip_vals   += repl_present

                if debug:
                    print("REPL ELEMENTS / REPL / SKIP CODES / SKIP VALS")
                    print(tuple(elements), repl_factor, skip_codes, skip_vals)
                # return elements list, replication factor and tuple containing skip information
                return elements, repl_factor, (skip_codes, skip_vals)
           
            # if everything goes well this bool should stay False; only for the rare case of wrong values or keys
            skip_file = False

            # this loop runs until one of the iteration causes a StopIteration error which we accept
            #continue_loop = True
            #while continue_loop:
            while True:
                try:
                    # iterate to get next code
                    code = next(codes)
                    # when location and datetime are already defined, get obs data
                    if location is not None and datetime is not None:
                        # if we come across a block number save the obs_list and to the data dict
                        if code == 1001: # blockNumber
                            if debug: print("STOP", location)
                            if obs_list:
                                if debug:
                                    print("OBS LIST:")
                                    print(location, datetime, obs_list)
                                # add the data to the current datetime; if it does not exist create
                                try:    obs_bufr[ID][location][datetime] += obs_list
                                except: obs_bufr[ID][location] = { datetime : obs_list }
                                
                                # obs_list gets cleared
                                obs_list = []
                                if debug: pdb.set_trace()

                            # iterate to next value
                            val = next(vals)
                            # retrieve new location, datetime and skip information
                            location,datetime,skip  = get_location_and_datetime(codes,code,vals,val)
                            skip_codes, skip_vals   = skip
                            # apply skip_codes and skip_vals provided by 'skip' tuple
                            for _ in range(skip_codes): next(codes)
                            for _ in range(skip_vals):  next(vals)
                        
                        # if we come across a scale or datasize alteration code we do not iterate
                        elif code in bf.scale_size_alter:
                            if debug: print("SCALE / DATASIZE ALTERATION!")
                            # only scale_alter information will be used to update precision of data
                            if code in bf.scale_alter:
                                # location and datetime need to have actual values not "" or None
                                if location and datetime:
                                    # if the previous code was not a scale increase append to obs
                                    if previous_code != 202129: obs_list.append( (code, None) )
                                    # else delete the last element of the obs_list
                                    else: del obs_list[-1]
                                    # remember previous code to compare later on
                                    previous_code = copy(code)
                        
                        # if the code is a replication information indicator (not 3100X though)
                        elif code in bf.repl_range:
                            #val = next(vals)
                            # try to get codes to repeat and replication factor (how many times?)
                            try: codes_repl, repl_factor, skip = get_repl_codes(codes, code, vals, val)
                            # if we come across a faulty value we need to skip the current file; therefore break
                            except ValueError:
                                skip_file = True
                                break
                            skip_codes, skip_vals           = skip
                            # skip codes and values again
                            if debug: print("SKIP CODES", skip_codes)
                            for _ in range(skip_codes): next(codes) 
                            if debug: print("SKIP VALUES", skip_vals)
                            for _ in range(skip_vals):  next(vals)

                            if debug: print("REPL:", repl_factor)

                            # aggregate 1 min values to 10 min
                            if repl_factor == 10:
                                if debug: print("REPL 10")
                                # the next 10 values are put into a list comprehension as follows
                                val_10 = [ next(vals) for _ in range(10) ]
                                if debug: print(val_10)
                                # this is necessary because stations that contain no data still need to be parsed
                                if location and datetime:
                                    # if we encounter a series of 10 precipitation values (amount or duration)
                                    if 13011 in codes_repl or 26020 in codes_repl:
                                        if debug: print(13011, codes_repl)
                                        # add the sum of all 10 values to the list of observations
                                        obs_list.append( (13011, np.nansum(val_10)) )
                                    # if there are 10 values of current weather statuses (ww codes)
                                    elif 20003 in codes_repl:
                                        if debug: print(20003, codes_repl)
                                        # add only the highest, most siginificant ww code to the obs list
                                        obs_list.append( (20003, np.nanmax(val_10)) )
                            else:
                                # repeat the elements in the list according to the replication factor
                                for _ in range(repl_factor):
                                    # iterate over every elements in the codes_repl list
                                    for code_r in codes_repl:
                                        # if we encounter a scale or size alteration code
                                        if code_r in bf.scale_size_alter:
                                            if debug: print("SCALE / DATASIZE ALTERATION!")
                                            # only save the scale changes but in any case skip a value (continue)
                                            if location and datetime and code in bf.scale_alter:
                                                obs_list.append( (code, None) )
                                            continue
                                        # move on to the next value in our main values iterator
                                        val_r = next(vals)
                                        if debug: print(bc.to_code(code_r), val_r)
                                        # only if location and datetime have a proper value (not None or "")
                                        if location and datetime:
                                            # if the code_r is relevant and is not a NaN: add it to the obs list
                                            if code_r in bf.relevant_codes and not np.isnan(val_r):
                                                obs_list.append( (code_r, val_r) )
                            
                            #if not repl_factor: next(vals)
                        
                        # if location and datetime are not None
                        else:
                            # we only want relevant codes and will skip the rest
                            if code in bf.relevant_codes:
                                # iterate to the next value
                                val = next(vals)
                                # again location and datetime must have actual values and value should not be NaN
                                if location and datetime and not np.isnan(val):
                                    # turn 1min values to 10min values
                                    if code == 4025 and val == -1: val = -10
                                    if debug: print(bc.to_code(code), val)
                                    # last but not least append the (code, value) tuple to the list of obs
                                    obs_list.append( (code, val) )
                            #
                            else:
                                if debug: print(bc.to_code(code))
                                # we always need to iterate to the next value at the end
                                next(vals)

                    # get blockNumber, stationNumber and datetime info
                    elif code == 1001:
                        # iterate to the next nalue
                        val = next(vals)
                        # get location, datetime and skip information
                        location,datetime,skip  = get_location_and_datetime(codes, code, vals, val)
                        skip_codes, skip_vals   = skip
                        # skip codes and values according to the information provided by the 'skip' tuple
                        for _ in range(skip_codes): next(codes)
                        for _ in range(skip_vals):  next(vals)
                    
                    #
                    else:
                        if debug: print(bc.to_code(code))
                        # also in this case we need to get the next value to match with next(codes)
                        next(vals)

                # if we encounter a StopIteration error we break the loop
                except StopIteration:   break
                else:                   continue
                #except StopIteration:   continue_loop = False; continue # break while loop
                #else:                   continue_loop = True            # continue while loop
            
            # end of while loop

            # if we need to skip the current file: continue to next one or, if it is the last, quit the for loop
            if skip_file: continue

            # if the obs list contains any data add it to the dictionary
            if obs_list:
                if debug:
                    print("OBS LIST:")
                    print(location, datetime, obs_list)
                # add to the dictionary if datetime exists; else create the datetime dict first
                try:    obs_bufr[ID][location][datetime] += obs_list
                except: obs_bufr[ID][location] = { datetime : obs_list }

        # end of with clause (closes file handle automatically)

        # free up memory again
        ec.codes_release(msg)
        
        # only if there was a new observation at least once in the dataset (new_obs has been set True) 
        if new_obs:
            # add the file as status==parsed to the file_statuses set and log the file's status as parsed
            file_statuses.add( ("parsed", ID) ); log.debug(f"PARSED: '{FILE}'")
        # else the file's status will be empty in the file_statuses and in the log file as well
        else: file_statuses.add( ("empty", ID) ); log.info(f"EMPTY:  '{FILE}'")
        
        #TODO fix memory leak or find out how restarting script works together with multiprocessing
        # check the amount of free RAM on the system and compare it to the min value defined in the settings
        memory_free = psutil.virtual_memory()[1] // 1024**2
        # if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            # initialize a new instant of the database class (establishes a new connection)
            db = dc(config=config_database)
            # bulk set all file statuses which have been saved previously in the file_statuses set
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=bf.timeout)
            # close without forcing a commit
            db.close()

            print("TOO MUCH RAM USED, RESTARTING...")
            # before restarting the script we need to convert the keys to the database format (if data present)
            if obs_bufr:
                obs_db = bf.convert_keys_us( obs_bufr, source, shift_datetime=False )
                # and if we received a dictionary back: insert its information the station databases
                if obs_db: obs.to_station_databases( obs_db, scale=True )
            
            # in case we used a PID file: remove it since it will be created anew after the restart
            if pid_file: os.remove( pid_file )
            # get the name of the currently running executable
            exe = sys.executable
            # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()

    # create a new instance of the database class
    db = dc(config=config_database)
    # bul set all file statuses
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=bf.timeout)
    # close the database connection again without committing to the database
    db.close()

    if debug: print( obs_bufr )
   
    # if any observation data have been recorded
    if obs_bufr:
        # convert the keys to the database format
        obs_db = bf.convert_keys_us( obs_bufr, source, shift_datetime=False )
        #for ID in obs: obs_bufr[ID].close()
        
        # if dictionary is not empty save its content to the station databases
        if obs_db: obs.to_station_databases( obs_db, scale=True, traceback=True )
    
    # remove file containing the pid, so the script can be started again
    if pid_file: os.remove( pid_file )


if __name__ == "__main__":
 
    # define program info message (--help, -h) and parser arguments with explanations on them (help)
    info    = "Decode one or more BUFR files and insert relevant observation data into station databases. "
    info   += "NOTE: Setting a command line flag or option always overwrites the setting from the config file!"
    # create argument parser object which lets us define command line arguments and parse them
    psr = argparse.ArgumentParser(description=info)
    
    # add all needed command line arguments to the program's interface
    psr.add_argument("-l","--log_level", choices=gv.log_levels, default="NOTSET", help="set logging level")
    psr.add_argument("-i","--pid_file", action='store_true', help="create a pid file to easily check if script is running")
    psr.add_argument("-f","--file", help="parse single file bufr file, will be handled as source=extra by default")
    psr.add_argument("-v","--verbose", action='store_true', help="show more detailed output")
    psr.add_argument("-p","--profiler", help="enable profiler of your choice (default: None)")
    #TODO replace profiler by number of processes (prcs) when real multiprocessing (using module) is implemented
    psr.add_argument("-c","--clusters", help="station clusters to consider, comma seperated")
    psr.add_argument("-C","--config", default="obs", help="set custom name of config file")
    psr.add_argument("-m","--max_retries", help="maximum attemps when communicating with station databases")
    psr.add_argument("-M","--mode", help="set operation mode; options available: {oper, dev, test}")
    psr.add_argument("-n","--max_files", type=int, help="maximum number of files to parse (per source)")
    psr.add_argument("-s","--sort_files", action='store_true', help="sort files alpha-numeric before parsing")
    psr.add_argument("-o","--timeout", help="timeout in seconds for station databases")
    psr.add_argument("-d","--debug", action='store_true', help="enable or disable debugging")
    psr.add_argument("-k","--skip", default="", help="skip [c]omputed, [f]unction and/or [d]uplicate keys")
    psr.add_argument("-t","--traceback", action='store_true', help="enable or disable traceback")
    psr.add_argument("-T","--tables", help="(absolute) path to ECCODES BUFR tables directory")
    psr.add_argument("-r","--redo", action='store_true', help="decode bufr again even if already processed")
    psr.add_argument("-R","--restart", help=r"only parse all files with status 'locked_{pid}'")
    psr.add_argument("source", default="", nargs="?", help="parse source / list of sources (comma seperated)")
    #TODO add shelve option to save some RAM
    
    # parse all command line arguments and make them accessible via the args variable
    args = psr.parse_args()

    # read configuration file into a dictionary
    config          = gf.read_yaml( args.config )
    # get the name of the currently running script
    script_name     = gf.get_script_name(__file__)
    # get the config for this very script
    config_script   = config["scripts"][script_name]
    # lookup the needed conda environment in the config
    conda_env       = os.environ['CONDA_DEFAULT_ENV']
    
    # if the currently active environment differs form prequirement in the script: exit showing error message
    if config_script["conda_env"] != conda_env:
        sys.exit(f"Script needs to run in conda environment {config_script['conda_env']}, exiting!")
    
    # save the general part of the configuration in a variable for easier acces
    config_general = config["general"]
    # do the same with the default bufr tables settings
    tables_default = config["bufr"]["tables"]
    
    # if there is a max_files setting present in command line options or config: use it preferring the cli value
    if args.max_files is not None:  config_script["max_files"]  = args.max_files
    # same for sort_files
    if args.sort_files: config_script["sort_files"] = args.sort_files
    
    # also for the PID file setting prefer the one from the command line arguments if present
    if args.pid_file: config_script["pid_file"] = True

    # if a PID file is used: check whether there is already a script with the same name running
    if config_script["pid_file"]:
        # get the pid of the currently running process
        pid = str(os.getpid())
        # the name of the PID file consists of script name plus extension '.pid'
        pid_file = script_name + ".pid"
        # if an already running instance of the script is detected: exit the execution immediately
        if gf.already_running( pid_file ): sys.exit(f"{script_name} is already running... EXITING!")
    # otherwise no PID file will be used
    else: pid_file = None
    
    # if a profiler module is defined: once more prefer the command line's option other the config dictionary
    if args.profiler: config_script["profiler"] = args.profiler
    # in case we are using a prfiler 
    if config_script["profiler"]:
        # importlib lets us dynamically load modules (see https://docs.python.org/3/library/importlib.html)
        import importlib
        # the profiler will be loaded into its own namespace (always 'profiler', regardless of what package name)
        profiler    = importlib.import_module(config_script["profiler"])
        # indicate that we are using a profiler
        profile     = True
    # no profiler is used
    else: profile = False
    
    # log level also prefers the setting from command line arguments
    if args.log_level: config_script["log_level"] = args.log_level
    # get a logger instance for the current script
    log = gf.get_logger(script_name)
    
    # remeber the starting time of the script so we can measure its performance later (we do this quite late...)
    start_time  = dt.utcnow()
    # a string that can be printed to log or stdout
    started_str = f"STARTED {script_name} @ {start_time}"; log.info(started_str)
    
    # verbose setting also gives priority to command line value
    if args.verbose is not None: config_script["verbose"] = args.verbose
    # shorthand for the setting
    verbose = config_script["verbose"]
    # if verbose we will now print out the string we created a few lines before
    if verbose: print(started_str)
    
    # once more the debug setting from command line comes first
    if args.debug:                  config_script["debug"] = True
    # if we debug we will use the pdb module always
    if config_script["debug"]:      import pdb; debug = True
    # else we provide a shorthand for lines in which debugging could be useful
    else:                           debug = False
    
    # same procedure as always for traceback: command line goes first
    if args.traceback:              config_script["traceback"] = traceback = True
    # traceback gets a shorthand as well because it will be used quite frequently to debug output from classes
    else:                           traceback = config_script["traceback"]
    
    # same for timeout
    if args.timeout:                config_script["timeout"] = timeout_station = args.timeout
    # the timeout setting is needed for database access and tells sqlite how long to wait fro the next try
    else:                           timeout_station = config_script["timeout"]
    
    # and maximum retries
    if args.max_retries:            config_script["max_retries"] = max_retries = args.max_retries
    # how many times do we want to try accessing the database?
    else:                           max_retries = config_script["max_retries"]
    
    # shorthand for the execution mode {dev, oper, test}
    mode    = config_script["mode"]
    # the general output path can differ from the one defined in scripts (or even per-source)
    output  = config["general"]["output"]

    # 'output' defined in script config has priority over general config
    if "output" in config_script: output = config_script["output"]
    
    # clusters also prefer the command line argument setting
    if args.clusters: config_source["clusters"] = args.clusters

    # get configuration for the initialization of the database class
    config_database = config["database"]

    # add files table (file_table) to main database if not exists
    #TODO this should be done during initial system setup, file_table should be added there
    db = dc(config=config_database)
    # create the file_table in the main database
    db.cur.execute( gf.read_file( "file_table.sql" ) )
    # close the connection again and commit the changes
    db.close(commit=True)

    # if processing a single file call the function with file argument
    if args.file: decode_bufr_ex( file=args.file, pid_file=pid_file )
    # else iterate over all sources in the config
    elif args.source:
        # source config can be accessed by its name in the sources section of the YAML
        source = config["sources"][args.source]
        # source argument can be a list which is indicated by a comma (sources are comma-seperated)
        if "," in source:
            # split the sources string and create a proper list
            sources = source.split(",")
            # create the empty 'config_sources' dictionary which stores all needed configurations
            config_sources = {}
            # iterate over all sources and save the settings into 'config_source'
            for s in sources: config_sources[s] = config["sources"][s]
        # if we use a single source only: create the  dictionary instantly
        else: config_sources = { args.source : config["sources"][args.source] }
    # if no source argument is given: process all sources that are present in the config file
    else: config_sources = config["sources"]
    
    # if we do not process a single file
    if not args.file:
        # iterate over all sources in the config
        for SOURCE in config_sources:
            if verbose: print(f"PARSING SOURCE: {SOURCE}")
            # run the program's main function, handing over source and pid_file (if desired)
            decode_bufr_ex( source = SOURCE, pid_file=pid_file )
    
    # measure the time when the script has (almost) stopped
    stop_time = dt.utcnow()
    # the finished_str consists of the script's name and its stop time
    finished_str = f"FINISHED {script_name} @ {stop_time}"
    # save it to the logs with priority 'info'
    log.info(finished_str)
    
    if verbose:
        print(finished_str)
        # measure the time taken for the execution of the script and print to stdout
        time_taken = stop_time - start_time
        print(f"TIME TAKEN: {time_taken.seconds}.{time_taken.microseconds} s")
