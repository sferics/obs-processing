#TODO metview (mv), perl/GeoBUFR (gb), plbufr flat mode (fl) ???

#TODO outsource all different BUFR main function versions to this file and import in respective script
#from decode_bufr_functions import decode_bufr_XX

#TODO do all the necessary imports at the beginning of each function (or wherever needed)

import global_variables as gv
import global_functions as gf

def decode_bufr_gt(ID, FILE, DIR, bf, log, traceback=False, debug=False, verbose=False):
    """
    """
    if debug: import pdb
    if not verbose:
        import warnings
        warnings.simplefilter(action='ignore', category=FutureWarning)
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.filterwarnings("ignore", module="plbufr")
        warnings.filterwarnings("ignore", module="ecmwflibs")
    import plbufr

    PATH = DIR + FILE

    generator, BufrFile = plbufr.read_bufr(PATH, columns=bf.relevant_keys, required_columns=bf.required_keys, filter_method=all, return_method="gen", skip_na=True)

    time_period = ""
    obs_bufr    = {}
    file_status = "empty"

    for row in generator:
        if debug: print("ROW", row)
        #TODO possibly BUG in pdbufr? timePeriod=0 never exists; write bug report in github!
        try:
            if row[bf.tp] is not None:
                time_period = row[bf.tp]
        except: pass

        try:
            repl_10 = row[bf.replication] == 10 or row[bf.ext_replication] == 10
            if time_period == -1 and repl_10 and (row[bf.ww] is not None or row[bf.rr] is not None):
                continue
        except: pass

        location = str(row[bf.wmo]) + "0"
        if location not in bf.known_stations: continue

        datetime = row[bf.dt]
        if datetime is None:
            if verbose: print("NO DATETIME:", FILE)
            continue

        if location not in obs_bufr:            obs_bufr[location]           = {}
        if datetime not in obs_bufr[location]:  obs_bufr[location][datetime] = {}

        modifier_list = []
        for key in (bf.obs_sequence, bf.sensor_height, bf.sensor_depth, bf.vertical_signf):
            try:
                if row[key] is not None:
                    modifier_list.append((key, row[key]))
            except: continue

        obs_list = []

        #for ignore_key in bf.ignore_keys:
        for ignore_key in bf.ignore_keys.intersection(set(row.keys())):
            try:    del row[ignore_key]
            except: pass

        for key in row:
            if row[key] is not None: obs_list.append((key, row[key]))

        if modifier_list and obs_list: obs_list = modifier_list + obs_list
        if obs_list:
            try:    obs_bufr[location][datetime][time_period] += obs_list
            except: obs_bufr[location][datetime][time_period] = obs_list
            file_status = "parsed"

    # close the file handle of the BufrFile object
    BufrFile.close()
    #stop_time = dt.utcnow()

    return obs_bufr, file_status


def decode_bufr_pl(ID, FILE, DIR, bf, log, traceback=False, debug=False, verbose=False):
    """
    """
    if debug: import pdb
    if not verbose:
        import warnings
        warnings.simplefilter(action='ignore', category=FutureWarning)
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.filterwarnings("ignore", module="plbufr")
        warnings.filterwarnings("ignore", module="ecmwflibs")
    import plbufr

    obs_bufr = {}
    file_status = "empty"

    PATH = DIR + FILE
    
    if hasattr(bf, "filters"):
        filters = bf.filters
    else: filters = {}

    df = plbufr.read_bufr(PATH, columns=bf.relevant_keys, required_columns=bf.required_keys, filters=filters,filter_method=all)#, skip_na=True)
    
    if df.width == 0: return {}, file_status

    #TODO use typical datetime if no datetime present (which should never happen with DWD OpenData BUFRs)
    #typical_datetime = "typical_datetime"
    time_period = ""
    cor         = 0

    cols        = df.columns
    cols_needed = [ i for i in df.columns if i not in bf.ignore_keys ]
    
    if debug: print("COLS NEEDED", cols_needed)

    for row in df.iter_rows(named=True, buffer_size=16384):
        if debug: print("ROW", row)
        #TODO possibly BUG in plbufr? timePeriod=0 never exists; write bug report in github!
        try:
            if row[bf.tp] is not None:
                time_period = row[bf.tp]
        except: pass

        try:
            repl_10 = row[bf.replication] == 10 or row[bf.ext_replication] == 10
            if time_period == -1 and repl_10 and (row[bf.ww] is not None or row[bf.rr] is not None):
                continue
        except: pass

        location = str(row[bf.wmo]) + "0"
        if location not in bf.known_stations: continue

        datetime = row[bf.dt]
        if datetime is None:
            if verbose: print("NO DATETIME:", FILE)
            continue

        if location not in obs_bufr:            obs_bufr[location]           = {}
        if datetime not in obs_bufr[location]:  obs_bufr[location][datetime] = {}

        modifier_list = []
        for key in (bf.obs_sequence, bf.sensor_height, bf.sensor_depth, bf.vertical_signf):
            try:
                if row[key] is not None:
                    modifier_list.append((key, row[key]))
            except: continue

        obs_list = []

        #row_needed = [ row[i] for i in range(len(row)) if i not in cols_noneed ]
        row_needed = [ row[i] for i in cols_needed ]

        for key, val in zip(cols_needed, row_needed):
            if val is not None: obs_list.append((key, val))

        if modifier_list and obs_list: obs_list = modifier_list + obs_list
        if obs_list:
            try:    obs_bufr[location][datetime][time_period] += obs_list
            except: obs_bufr[location][datetime][time_period] = obs_list
            file_status = "parsed"

    return obs_bufr, file_status


def decode_bufr_pd(ID, FILE, DIR, bf, log, traceback=False, debug=False, verbose=False):
    """
    """
    if debug: import pdb
    if not verbose:
        import warnings
        warnings.simplefilter(action='ignore', category=FutureWarning)
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.filterwarnings("ignore", module="pdbufr")
        warnings.filterwarnings("ignore", module="ecmwflibs")
    import pdbufr
    import pandas as pd

    obs_bufr = {}
    file_status = "empty"

    PATH = DIR + FILE

    df = pdbufr.read_bufr(PATH, columns=bf.relevant_keys, required_columns=bf.required_keys)

    # len(df.index) == 0 is much faster than df.empty or len(df) == 0
    # https://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty
    # if the dataframe contains no data or no stations with WMO IDs, skip
    if len(df.index) == 0 or df.loc[:, bf.wmo].isna().all():
        return {}, file_status

    # if dataframe larger than minimum keyset: drop all rows and columns which only contains NaNs
    #elif len(df.columns) > number_of_filter_keys:
    else:
        df.dropna(how="all", inplace=True)
        #print(df.shape)
        if len(df.index) == 0:
            return {}, file_status
        #df.dropna(how="all", axis=1, inplace=True)
        #print(df.shape)

    #TODO use typical datetime if no datetime present
    #typical_datetime = "typical_datetime"
    time_period = ""
    cor         = 0

    for ix, row in df.iterrows():

        #keys_not_na = bufr_obs_keys.intersection(row.index)
        # in future versions of pandas we will need this next line:
        #keys_not_na = list(bufr_obs_keys.intersection(row.index))

        #if row.loc[keys_not_na].isna().all(): continue

        #keys_not_na = bufr_obs_keys.intersection(row.index)
        #if not row.loc[bufr_obs_keys].notna().any(): continue

        #TODO possibly BUG in pdbufr? timePeriod=0 never exists; write bug report in github!
        try:
            if pd.notna(row[bf.tp]): time_period = row[bf.tp]
        except: pass

        try:
            repl_10 = row[bf.replication] == 10 or row[bf.ext_replication] == 10
            if time_period == -1 and repl_10 and (pd.notna(row[bf.ww]) or pd.notna(row[bf.rr])):
                continue
        except: pass

        location = str(row[bf.wmo]) + "0"
        if location not in bf.known_stations: continue

        datetime = row[bf.dt]
        if pd.isna(datetime):
            if verbose: print("NO DATETIME:", FILE)
            continue

        for i in (bf.replication, bf.short_replication, bf.ext_replication, bf.tp, bf.wmo, bf.dt):
            try:    del row[i]
            except: continue

        if location not in obs_bufr:            obs_bufr[location]           = {}
        if datetime not in obs_bufr[location]:  obs_bufr[location][datetime] = {}

        modifier_list = []
        for key in (bf.obs_sequence, bf.sensor_height, bf.sensor_depth, bf.vertical_signf):
            try:
                if pd.notna(row[key]): modifier_list.append((key, row[key]))
            except: continue
            else:   del row[key]

        obs_list = []
        for key, val in zip(row.index, row):
            if pd.notna(val): obs_list.append((key, val))

        if modifier_list and obs_list: obs_list = modifier_list + obs_list
        if obs_list:
            try:    obs_bufr[location][datetime][time_period] += obs_list
            except: obs_bufr[location][datetime][time_period] = obs_list
            file_status = "parsed"

    return obs_bufr, file_status


def decode_bufr_us(ID, FILE, DIR, bf, log, traceback=False, debug=False, verbose=False):
    """
    """
    if not verbose:
        import warnings
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.filterwarnings("ignore", module="ecmwflibs") 
    import eccodes as ec
    if debug: import pdb

    with open(DIR + FILE, "rb") as file_handle:
        try:
            msg = ec.codes_bufr_new_from_file(file_handle)
            #msg = ec.codes_new_from_file(file_handle, ec.CODES_PRODUCT_BUFR)
            if msg is None:
                if verbose: print(f"EMPTY:  '{FILE}'")
                return {}, "empty"
            # skip extra attributes like units and scale to decode 25% faster (we can get them via key->code)
            # https://confluence.ecmwf.int/display/UDOC/Performance+improvement+by+skipping+some+keys+-+ecCodes+BUFR+FAQ)
            ec.codes_set(msg, 'skipExtraKeyAttributes', 1)
            ec.codes_set(msg, "unpack", 1)
            # extract all subsets
            ec.codes_set(msg, "extractSubsetIntervalStart", 1)
            ec.codes_set(msg, "extractSubsetIntervalEnd", ec.codes_get_long(msg, "numberOfSubsets"))
            ec.codes_set(msg, "doExtractSubsets", 1)
        except Exception as e:
            log_str = f"ERROR:  '{FILE}' ({e})"
            log.error(log_str)
            if verbose: print(log_str)
            if traceback: gf.print_trace(e)
            return {}, "error"
        
        obs_bufr    = {} #shelve.open(f"shelves/{ID}", writeback=True)
        file_status = "empty"

        iterid = ec.codes_bufr_keys_iterator_new(msg)
        
        ec.codes_skip_computed(iterid)
        ec.codes_skip_function(iterid)
        ec.codes_skip_duplicates(iterid)

        meta, typical   = {}, {}
        valid_obs       = False
        location        = None
        skip_next       = 10
        subset          = 0
        skip_obs        = False
        last_key        = None

        if debug: pdb.set_trace()

        while ec.codes_bufr_keys_iterator_next(iterid):

            if skip_next: skip_next -= 1; continue

            key = ec.codes_bufr_keys_iterator_get_name(iterid)

            if last_key == "typical" and last_key not in key:
                last_key = None; skip_next = 3; continue

            if key == "subsetNumber":
                if subset > 0:
                    meta = {}; location = None; valid_obs = False; skip_obs = False
                subset += 1; continue
            elif skip_obs: continue

            clear_key = bf.clear(key)
            if clear_key not in bf.relevant_keys: continue
            #if debug: print(key)

            if valid_obs:
                if location not in obs_bufr:
                    obs_bufr[location] = {}

                if clear_key in bf.obs_time_keys:
                    try: value = ec.codes_get( msg, key )
                    except Exception as e:
                        if verbose: print(FILE, key, e)
                        if traceback: gf.print_trace(e)
                        log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                        if verbose: print(log_str)
                        continue

                    # skip 1min ww and RR which are reported 10 times; 10min resolution is sufficient for us
                    if clear_key in {bf.replication, bf.ext_replication}:
                        if value == 10: skip_next = 10
                        continue

                    if value not in bf.null_vals:

                        # get the BUFR code number which gives us all necessary unit and scale info
                        code        = ec.codes_get_long( msg, key + "->code" )
                        obs_data    = ( code, value )

                        #TODO use a defaultdict instead? https://realpython.com/python-defaultdict
                        #obs_bufr = defaultdict(lambda:dict())
                        #TODO find out which way is faster (dict, defaultdict, if/else or try/except)
                        """
                        if datetime not in stations[location]:
                            stations[location][datetime] = []
                        stations[location][datetime].append( obs_data )
                        """
                        try:    obs_bufr[location][datetime].append(obs_data)
                        except: obs_bufr[location][datetime] = [obs_data]
                        """
                        # avoid duplicate modifier keys (like timePeriod or depthBelowLandSurface)
                        if clear_key in bf.modifier_keys:
                            try:
                                if clear_key == obs_bufr[location][datetime][-2][0]:
                                    del obs_bufr[location][datetime][-2]
                            except: pass
                        """
                        file_status = "parsed"

            else:
                if not subset and key in bf.typical_keys:
                    typical[key]    = ec.codes_get( msg, key )
                    if typical[key] in bf.null_vals: del typical[key]
                    last_key        = "typical"
                    continue

                if location is None and clear_key in bf.station_keys:
                    #meta[clear_key] = ec.codes_get(msg, key)
                    try: meta[clear_key] = ec.codes_get(msg, key)
                    except: meta[clear_key] = ec.codes_get_array(msg, key)[0]
                    #TODO some OGIMET-BUFRs seem to contain multiple station numbers in one key (arrays)

                    if meta[clear_key] in bf.null_vals:
                        del meta[clear_key]; continue

                    # check for identifier of DWD stations (in German: "nebenamtliche Stationen")
                    if "shortStationName" in meta:
                        location = meta["shortStationName"]
                        station_type = "dwd"; skip_next = 4

                    # check if all essential station information keys for a WMO station are present
                    elif { "stationNumber", "blockNumber" }.issubset( set(meta) ):
                        location = bf.to_wmo(meta["blockNumber"], meta["stationNumber"])
                        station_type = "wmo"
                        if "skip1" in bf.config: skip_next = cf.bufr["skip1"]
                        #TODO put in config
                        elif bf.source in {"DWD","COD","NOAA"}: skip_next = 2

                    if location and location not in bf.known_stations:
                        meta = {}; location = None; skip_obs = True
                        if station_type == "dwd": skip_next = 13
                        elif "skip2" in bf.config: skip_next = bf.config["skip2"]
                        #TODO put in config
                        elif bf.source in {"DWD","COD","KNMI","RMI","NOAA"}:
                            if station_type == "wmo":   skip_next = 11

                elif location:

                    if clear_key in bf.set_time_keys: # {year, month, day, hour, minute}
                        meta[clear_key] = ec.codes_get_long(msg, key)
                        if meta[clear_key] in bf.null_vals: del meta[clear_key]

                        if clear_key == "minute":
                            # check if all essential time keys are now present
                            valid_obs = bf.set_time_keys.issubset(meta)
                            if valid_obs:
                                datetime = gf.to_datetime(meta)
                                if debug: print(meta)
                                if "skip3" in bf.config: skip_next = bf.config["skip3"]
                                #TODO put in config
                                elif bf.source in {"DWD","COD","NOAA"}: skip_next = 4
                                continue

                            elif bf.set_time_keys_hour.issubset(meta):
                                # if only minute is missing, assume that minute == 0
                                meta["minute"] = 0; valid_obs = True
                                datetime = gf.to_datetime(meta)
                                if debug: print("minute0:", meta)
                                continue

                            # if we are still missing time keys: use the typical information
                            elif typical:
                                # use the typical values we gathered earlier keys are missing
                                for i,j in zip(bf.time_keys, bf.typical_keys):
                                    try:    meta[i] = int(typical[j])
                                    except: pass

                                # again, if only minute is missing, assume that minute == 0
                                if bf.set_time_keys_hour.issubset(meta):
                                    meta["minute"] = 0; valid_obs = True; continue

                                # no luck? possibly, there could be typicalDate or typicalTime present
                                if not {"year","month","day"}.issubset(set(meta)) and "typicalDate" in typical:
                                    typical_date    = typical["typicalDate"]
                                    meta["year"]    = int(typical_date[:4])
                                    meta["month"]   = int(typical_date[4:6])
                                    meta["day"]     = int(typical_date[-2:])
                                else: skip_obs = True; continue

                                if ("hour" not in meta or "minute" not in meta) and "typicalTime" in typical:
                                    typical_time    = typical["typicalTime"]
                                    if "hour" not in meta:      meta["hour"]    = int(typical_time[:2])
                                    if "minute" not in meta:    meta["minute"]  = int(typical_time[2:4])
                                else: skip_obs = True; continue

                            else: skip_obs = True


        # end of while loop
        ec.codes_keys_iterator_delete(iterid)

    # end of with clause (closes file handle)
    ec.codes_release(msg)
    
    return obs_bufr, file_status


def decode_bufr_ex(ID, FILE, DIR, bf, log, traceback=False, debug=False, verbose=False):
    """
    """
    if not verbose:
        import warnings
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.filterwarnings("ignore", module="ecmwflibs")
    import eccodes as ec
    import numpy as np
    from itertools import cycle
    from copy import copy
    from datetime import datetime as dt
    import sys
    if debug: import pdb

    obs_bufr        = {}
    file_status     = "empty"
    error_message   = "unknown error"

    def get_error_message(error_type):
        """
        """
        match error_type:
            case ValueError():
                return "missing replication factor"
            case KeyError():
                return "unknown sequence number"
            case _:
                return error_message

    def get_num_elements(code):
        """
        """
        return int((code - 100000) / 1000)

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
        num_elements = get_num_elements(code)
        # number of repetitions (1-999) -> replication factor
        repl_factor  = int((code - 100000) - (num_elements * 1000))
        if debug: print(num_elements, repl_factor)
        # return extracted information as tuple
        return num_elements, repl_factor


    def get_location_and_datetime(codes, code, vals, val):
        """
        Parameter:
        ----------
        codes : iterator of BUFR codes
        code : current BUFR code
        vals : iterator of values in dataset
        val : current value

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
            location = bf.to_wmo( block, station )
        # otherwise, we do not have a valid WMO location
        else: return "", "", (0, 0)

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


    with open(DIR + FILE, "rb") as file_handle:
        try:
            # the file ID is the representation of the file in the main database (file_table)
            # if for whatever reason no ID (database lock?) or filestatus means skip:
            # continue with next file
            if not ID: return {}, 0, file_status
            # let ECCODES read the file into memory and handle it
            msg = ec.codes_bufr_new_from_file(file_handle)
            # msg should be an integer (ID); if not it is None (empty) and needs to be skipped
            if msg is None:
                # status is empty if the bufr file contains no data
                if verbose: print(f"EMPTY:  '{FILE}'")
                return {}, "empty"
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
            return {}, "error"
        # if all went down smoothly create an empty dictionary to store the data in it
        else: obs_bufr = {} #shelve.open(f"shelves/{ID}", writeback=True)

        # get descriptor data (unexpanded descriptors plus their values)
        vals    = ec.codes_get_double_array(msg, "numericValues")
        unexp   = ec.codes_get_long_array(msg, "unexpandedDescriptors")

        codes_exp = [] # expanded list of codes / descriptors
        # value and code indices
        pos_val, pos_code = 0, 0
        # previous code is None at first and will be needed to spot repeated sequences
        prev_code = None

        #codes_debug = {307096, 302084, 302085}

        skip_expand_seq = 0

        # as long as we did not reach the and of the unexpanded data array; continue
        while pos_code < len(unexp):
            # get code by subscripting position in the unexp array
            code = unexp[pos_code]
            #if code in codes_debug: sys.exit(code)
            # if the code is a sequence but no replication info sits directly in front of it
            if code in bf.sequence_range and not skip_expand_seq:
                # replace sequence codes by actual sequences and apply replication factors
                codes_seq = bf.bufr_sequences[code]
                # add code sequence to list of expanded descriptors
                codes_exp += codes_seq
            # else just add the code to the list
            else: codes_exp.append(code)
            # count up the position indicator
            pos_code += 1
            # remember the previous code to check whether it contains replication info
            prev_code = copy(code)
            if code in bf.repl_range:
                skip_expand_seq = get_num_elements(code)
            if code not in bf.repl_codes:
                skip_expand_seq += 1

        if debug:
            print("VALUES:")
            print( [ i if i != -1e+100 else "" for i in vals ] )
            print("CODES UNEXP:")
            print(list(unexp))
            print("CODES EXP:")
            print(codes_exp)
            pdb.set_trace()

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
        prev_code = unexp[0]

        #TODO instead of defining the following functions and using standard iterators we could also write
        #our own (complex) generator function with explicit return and/or yield from statements or decorators

        def get_repl_factor(code, val):
            """
            """
            # check the next_code, whether it provides a (short/extended) replication factor
            match code:
                case 31000: # short delayed replication factor
                    # if the next value is not zero or contains a NaN set repl_factor to 1
                    if val and not np.isnan(val):   repl_factor = 1
                    else:                           repl_factor = 0
                case 31001 | 31002: # (extended) delayed replication factor
                    # if the next value is not zero or NaN: set repl_factor to value itself
                    if val and not np.isnan(val):   repl_factor = int(val)
                    else:                           repl_factor = 0
                case _: # this case should rarely occur and actually only in case of faulty BUFR data
                    # in case we get some other code; release BUFR and skip this file to be save
                    ec.codes_release(msg)
                    # log an error message with the file that is currently processed + 'code'
                    log_str = f"ERROR:  '{FILE}' (MISSING REPLICATION FACTOR CODE '{code}')"
                    log.error(log_str)
                    if verbose or debug: print(log_str)
                    # set file status to 'error' in order to be able to track down what went wrong
                    file_status = "error"
                    # raising a ValueError enables us to exit the function and except the error outwards
                    raise ValueError
            
            if debug: print(code, repl_factor)
            return repl_factor

        def get_repl_codes(codes_repl, code, vals_repl, val):
            """
            Parameter:
            ----------
            codes_repl : iterator of BUFR codes
            code : current BUFR code
            vals_repl : iterator of values in dataset
            val : current value

            Notes:
            ------

            Return:
            -------
            elements, repl_factor, (skip_codes, skip_vals)
            """
            if debug:
                print("GET REPL")
                print( bf.to_code(code), val)
            
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
                repl_factor     = get_repl_factor(next_code, next_val)
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
                if debug: print("1 ELEM")
                next_code = next(codes_repl)
                if next_code in bf.sequence_range:
                    if debug: print("SEQ 1", next_code)
                    # if next_code is a BUFR sequence get the elements it contains
                    elements = bf.bufr_sequences[next_code]
                else:
                    # otherwise elements is just a single-element list
                    elements = [next_code]
            else:
                ## get the next elements in range and save them into a list comprehension
                #elements = [ next(codes_repl) for _ in range(num_elements) ]
                
                # because sequences can also contain nested repl information we need a for loop
                elements    = []
                prev_code   = None

                for i in range(num_elements):
                    next_code_i = next(codes_repl)

                    if prev_code not in bf.repl_info and next_code_i in bf.sequence_range:
                        if debug: print("SEQ I", next_code_i)
                        # if code is a sequence: get contained codes and add them
                        elements_i  = bf.bufr_sequences[next_code_i]
                        if debug: print("ELEMENTS I", elements_i)
                        elements    += elements_i
                        skip_codes  += 1
                        #TODO the sequence could also contain repl, what then? (never happens?)
                    else:
                        # otherwise add just a single element
                        elements.append(next_code_i)
            
                    prev_code = copy(next_code_i)
                
                skip_codes -= 1; skip_vals -= 1

            # only if a replication factor is given directly in the code: skip one value
            skip_vals += repl_present

            if debug:
                print("REPL ELEMENTS / REPL / SKIP CODES / SKIP VALS")
                print(elements, repl_factor, skip_codes, skip_vals)
            # return elements list, replication factor and tuple containing skip information
            return elements, repl_factor, (skip_codes, skip_vals)

        # if everything goes well this bool should stay False; only for the rare case of wrong values or keys
        skip_file       = False
        continue_loop   = True
        
        # this loop runs until one of the iteration causes a StopIteration error - which we accept
        while continue_loop:

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
                            try:    obs_bufr[location][datetime] += obs_list
                            except: obs_bufr[location] = { datetime : obs_list }

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
                                if prev_code != 202129: obs_list.append( (code, None) )
                                # else delete the last element of the obs_list
                                else: del obs_list[-1]
                                # remember previous code to compare it later on
                                prev_code = copy(code)

                    # if the code is a replication information indicator (not 3100X though)
                    elif code in bf.repl_range:
                        #val = next(vals)
                        # try to get codes to repeat and replication factor (how many times?)
                        try: codes_repl, repl_factor, skip = get_repl_codes(codes, code, vals, val)
                        # if we come across a faulty/unknown value we need to skip the current file
                        except Exception as error_type:
                            skip_file       = True
                            file_status     = "error"
                            error_message   = get_error_message(error_type)
                            break
                        
                        skip_codes, skip_vals = skip
                        # skip codes and values again
                        if debug: print("SKIP CODES", skip_codes)
                        for _ in range(skip_codes): next(codes)
                        if debug: print("SKIP VALUES", skip_vals)
                        for _ in range(skip_vals):  next(vals)

                        if debug: print("REPL:", repl_factor)

                        # aggregate 1 min values to 10 min
                        if repl_factor == 10:
                            if debug: print("REPL 10")
                            # the next 10 values will be put into a list comprehension as follows
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
                                
                                skip_next_r = 0

                                # iterate over every element in the codes_repl list
                                for idx, code_r in enumerate(codes_repl):
                                    
                                    if code_r in bf.sequence_range:
                                        skip_next_r -= 1

                                    if skip_next_r:
                                        skip_next_r -= 1
                                        if debug: print("SKIPPED:", bf.to_code(code_r))
                                        continue

                                    # if we encounter a scale or size alteration code
                                    if code_r in bf.scale_size_alter:
                                        if debug: print("SCALE / DATASIZE ALTERATION!")
                                        # only save the scale changes but in any case skip a value (continue)
                                        if location and datetime and code in bf.scale_alter:
                                            obs_list.append( (code, None) )
                                        continue
                                    
                                    # if there is a nested replication we need to go deeper into the rabbit hole...
                                    elif code_r in bf.repl_range:
                                        
                                        codes_r = iter(codes_repl[idx+1:])
                                        if debug: print("CODES R", codes_repl[idx+1:])

                                        try: codes_repl_r, repl_factor_r, skip_r = get_repl_codes(codes_r, code_r, vals, val)
                                        # if we come across a faulty/unknown value we need to skip the current file
                                        except Exception as error_type:
                                            skip_file       = True
                                            file_status     = "error"
                                            error_message   = get_error_message(error_type)
                                            continue_loop   = False
                                            # break the for loop and then the outer while loop
                                            break

                                        skip_codes_r, skip_vals_r = skip_r
                                        # skip only values again
                                        if debug: print("SKIP VALUES R",    skip_vals_r)
                                        for _ in range(skip_vals_r):        next(vals)
                                        
                                        if debug: print("REPL R", repl_factor_r)

                                        for _ in range(repl_factor_r):
                                            for code_rr in codes_repl_r:
                                                val_rr = next(vals)
                                                if debug: print(bf.to_code(code_rr), val_rr)
                                                # only if location and datetime have a proper value (not None or "")
                                                if location and datetime:
                                                    # if the code_rr is relevant and is not a NaN: add it to the obs list
                                                    if code_rr in bf.relevant_codes and not np.isnan(val_rr):
                                                        if debug and code_rr == 1023 and val_rr:
                                                            print("COR RR", val_rr)
                                                        obs_list.append( (code_rr, val_rr) )
                                        
                                        skip_next_r = len(codes_repl_r)
                                        if debug: print("CONTINUE", skip_next_r)
                                        continue

                                    # move on to the next value in our main values iterator
                                    val_r = next(vals)
                                    if debug: print(bf.to_code(code_r), val_r)
                                    # only if location and datetime have a proper value (not None or "")
                                    if location and datetime:
                                        # if the code_r is relevant and is not a NaN: add it to the obs list
                                        if code_r in bf.relevant_codes and not np.isnan(val_r):
                                            if debug and code_r == 1023 and val_r:
                                                print("COR R", val_r)
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
                                if debug: print(bf.to_code(code), val)
                                # last but not least append the (code, value) tuple to the list of obs
                                obs_list.append( (code, val) )
                                if debug and code == 1023 and val:
                                    print("COR", val)
                        #
                        else:
                            if debug: print(bf.to_code(code))
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
                    if debug: print(bf.to_code(code))
                    # also in this case we need to get the next value to match with next(codes)
                    next(vals)

            # if we encounter a StopIteration error we just break the while loop
            except StopIteration:   break
            else:                   continue

        # end of while loop

        # if we need to skip the current file: continue to next one or, if it is the last, quit the for loop
        if skip_file: return {}, file_status#, error_message

        # if the obs list contains any data add it to the dictionary
        if obs_list:
            if debug:
                print("OBS LIST:")
                print(location, datetime, obs_list)
            # add to the dictionary if datetime exists; else create the datetime dict first
            try:    obs_bufr[location][datetime] += obs_list
            except: obs_bufr[location] = { datetime : obs_list }
            if file_status != "error": file_status = "parsed"

    # end of with clause (closes file handle automatically)

    # free up memory again
    ec.codes_release(msg)
    
    return obs_bufr, file_status
