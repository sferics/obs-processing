#!/usr/bin/env python
import sys
import os
from obs import ObsClass as oc
from database import DatabaseClass as dc
from config import ConfigClass as cc
import global_functions as gf


def decimal_to_degrees(value, direction):
    """
    """
    if direction == "lat":
        cardinal_pos    = "N"
        cardinal_neg    = "S"
        degrees_rjust   = 2
    else:
        cardinal_pos    = "E"
        cardinal_neg    = "W"
        degrees_rjust   = 3
    
    degrees = int(value)
    minutes = int(np.round((value - degrees) * 0.6))
    
    if degrees >= 0:    cardinal = "E"
    else:               cardinal = "W"
    
    return f"{cardinal}{degrees.rjust(degrees_rjust, '0')} {minutes.just(2, '0')}"


def export_obs(stations, datetime_in):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    for loc in stations:
        # get all needed station metadata 
        db = dc( config=cf.database, ro=True )
        station_name = db.get_station_name(loc)
        station_icao = get_station_icao(loc)
        station_lat  = decimal_to_degrees(get_station_latitude(loc), "lat")
        station_lon  = decimal_to_degrees(get_station_longitude(loc), "lon")
        station_elev = int(np.round(get_station_elevation(loc)))
        db.close(commit=False)

        db_file = obs.get_station_db_path(loc)
        #db_file = f"{output}/{mode}/forge/{loc[0]}/{loc}.db"
        try: db_loc = dc( db_file, {"verbose":verbose, "traceback":traceback}, ro=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            if debug:       pdb.set_trace()
            continue
        
        # get all values of desired datetimes (hourly, 30min etc) which are not exported yet
        datetime_in = datetime_in.replace("datetime", "timestamp")
        
        sql = f"SELECT DISTINCT datetime,element,value FROM obs WHERE {datetime_in}"
        if not redo:
            sql += " AND exported = 0"
        
        db_loc.exe(sql)
        file_content = [] 

        # iterated over all these datetimes, starting with the oldest
        for row in sorted(db_loc.fetch()):
            datetime    = row[0]
            element     = row[1]
            value       = row[2]
            print("DATABASE")
            print(datetime, element, value) 
             
            metwatch_obs                    = {}
            metwatch_obs["YYYYMMDDhhmm"]    = datetime
            metwatch_obs["LOCATOR"]         = station_icao
            metwatch_obs["NAME"]            = station_name
            metwatch_obs["LAT"]             = station_lat
            metwatch_obs["LON"]             = station_lon
            metwatch_obs["ELEV"]            = station_elev
            metwatch_obs["TYPE"]            = "SYNO"

            # get all relevant data to export and write it to csv files in legacy output directory
            for element in elements:
                print("METWATCH")
                print(metwatch_export[element][0])
                
                metwatch_element    = metwatch_export[element][0]
                TR                  = metwatch_export[element][1]
                
                # if TR not present yet for this datetime, add it to the dict
                if TR is not None and "TR" not in metwatch_obs:
                    metwatch_obs["TR"] = TR
                
                metwatch_obs[metwatch_element] = value
        
            line = ""

            for key, val in metwatch_elements:
                print(key, val)
                for element, length in metwatch_header:
                    try:    value = metwatch_obs[element]
                    except: value = "/"
                    line += value.rjust(length, " ") + ";"
            
            file_content.append(line)
        
        # close database connection without committing
        db_loc.close(commit=False)
        
        print(header_line)
        
        # if -s/--sort_files flag is set: sort lines of file by datetime (YYYYMMDDhhmm)
        if sort_files: file_content.sort( key = lambda x : x[73:85] )
        
        print("\n".join(file_content))
        
        # final output: header line + file_content

    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Export (latest) observations from databases to legacy output format (metwatch csv)"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","V","r","s")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, clusters=True)
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
    mode            = cf.script["mode"]
    output          = cf.script["output"]
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]
    values          = cf.script["values"]
    sort_files      = cf.script["sort_files"]
    redo            = cf.args.redo
    sources         = cf.args.source
    
    metwatch_transl = gf.read_yaml("translations/metwatch")
    metwatch_header = metwatch_transl["header"]
    metwatch_export = metwatch_transl["export"]
    header_line     = ";".join( (name.rjust(l, " ") for name, l in metwatch_header.items()) ) + ";"

    if "min" in values:
        frequency   = int( values.replace("min", "") )
        minutes     = tuple( str(i).rjust(2, "0") for i in range(0, 60, frequency) )
        datetime_in = f"strftime('%M', datetime) IN {minutes}"
    elif "h" in values:
        frequency   = int( values.replace("h", "") )
        hours       = tuple( str(i).rjust(2, "0") for i in range(0, 24, frequency) )
        datetime_in = f" strftime('%M', datetime) = '00' AND strftime('%H', datetime) IN {hours}"
     
    #TODO implement WHERE dataset='{source}' or AND dataset='{source}' in all SELECT statements
    if len(sources) > 0:
        sql             = dc.sql_equal_or_in(sources)
        and_dataset     = f" AND dataset {sql}"
        where_dataset   = f" WHERE dataset {sql}"
    else:
        and_dataset, where_dataset = "", ""
    
    obs             = oc( cf, mode=mode, stage="final", verbose=verbose )
    db              = dc( config=cf.database, ro=True )
    stations        = db.get_stations( clusters )
    elements        = tuple(db.get_elements(path_identifier="export")) 
    db.close(commit=False)
    
    if processes: # number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=export_obs, args=(station_group,datetime_in))
            p.start()

    else: export_obs(stations)
