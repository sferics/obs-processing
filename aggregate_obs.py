#!/usr/bin/env python
import os
import sys
from datetime import datetime as dt, timedelta as td
#from obs import ObsClass
from database import DatabaseClass as dc
from config import ConfigClass as cc
import global_functions as gf
import global_variables as gv

# constants
mins_10 = td(minutes=10)

def aggregate_obs(stations, update=False):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    def get_distinct_years():
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe( f"SELECT DISTINCT strftime('%Y', datetime) FROM obs WHERE element = '{el_old}'" )
        return db_loc.fetch()

    def get_distinct_months():
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe((f"SELECT DISTINCT strftime('%m', datetime) FROM obs WHERE element = '{el_old}' "
                        f"AND strftime('%Y', datetime) = '{year}'"))
        return db_loc.fetch()


    def get_distinct_days():
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe((f"SELECT DISTINCT strftime('%d', datetime) FROM obs WHERE element='{el_old}' AND "
                        f"strftime('%Y', datetime) = '{year}' AND strftime('%m', datetime) = '{month}'"))
        return db_loc.fetch()


    def get_distinct_hours():
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe((f"SELECT DISTINCT strftime('%H', datetime) FROM obs WHERE element='{el_old}' AND strftime('%Y', "
            f"datetime) = '{year}' AND strftime('%m', datetime) = '{month}' AND strftime('%d', datetime) = '{day}'"))
        return db_loc.fetch()

    def get_copy_vals(year=None, month=None, day=None, offset=0, dur=None):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT * FROM obs WHERE element = '{el_old}' AND duration = '{dur}'"
         
        if all((year, month, day, dur)):
            
            if "h" in dur:
                dur_num = int(dur.replace("h",""))
            else:
                dur_num = int(dur.replace("min","")) / 60

            dt_start    = dt(int(year), int(month), int(day)) + td(hours=offset)
            dt_end      = dt_base + td(hours=dur_num) + td(hours=offset)
            
            sql += f" AND datetime(datetime) BETWEEN '{dt_start}' AND '{dt_end}'"
        
        db_loc.exe(sql)
        
        return db_loc.fetch()

    def get_duration(FUN, dur, subdur):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if FUN in {"MIN","MAX"}:
            if "h" in subdur:
                sql = "REPLACE(duration, 'h', '')"
            else:
                sql = "REPLACE(duration, 'min', '')"
            sql += f" <= '{subdur}'"
        else: sql = "duration='{subdur}'"

        return sql

    # main SQL insert statement which will be used at the end of aggregate_obs function
    SQL = "INSERT INTO obs VALUES(?,?,?,?,?) ON CONFLICT DO "
    if update: # update flag which forces already existing values to be updated
        SQL += "UPDATE SET value = excluded.value, duration = excluded.duration"
    else: # if -u/--update flag is not set do nothing
        SQL += "NOTHING"

    #TODO this function should make sure that in the forge stage we only have 30 min data resolution
    # that means if there are missing 0min or 30min values, we should replace them with the closest values, e.g.
    # 10min/50min for 0min and 20min/40min for 30min; TODO which of those should we prefer and why? average them?

    for loc in stations:
        
        sql_values = set()
        
        db_file = f"{output}/forge/{loc[0]}/{loc}.db"
        try: db_loc = dc( db_file, {"verbose":verbose, "traceback":traceback} )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue
        
        """
        #TODO remove this temporary fix!
        # delete all aggregated elements before calculating them again
        sql = f"DELETE FROM obs WHERE element {in_elems}"
        try:    db_loc.exe( sql )
        except Exception as e:
            print(sql)
            print(e)
            gf.print_trace(e)
        """

        for el_old in instant_elems:
            
            years_present = get_distinct_years()

            for year in years_present:

                months_present = get_distinct_months()

                for month in months_present:

                    days_present = get_distinct_days()

                    for day in days_present:

                        hours_present = get_distinct_hours()
                        
                        for hour in hours_present:
                            # check whether half-hourly data is present
                            # if not fill in (the mean) of 1 or 2 +/- 10min vals (when present)
                            # so if only +10min or -10min value is available, take it
                            # OR when they both exists, take their average
                            # actually, both cases are the same because we just use AVG()
                            for minute in (0,30):
                                
                                dt_base = dt(int(year), int(month), int(day), int(hour), minute)
                                
                                sql = f"SELECT ? FROM obs WHERE element = '{el_old}' AND datetime(datetime) = '{dt_base}'"
                                db_loc.exe( sql.replace("?", "COUNT(value), dataset") )
                                
                                # get number of values and dataset name
                                fetched = db_loc.fetch1() 
                                if fetched is None: continue

                                n_vals, dataset = fetched
                                # if no value is present
                                if not n_vals:
                                    dt_start    = dt_base - mins_10
                                    dt_end      = dt_base + mins_10     
                                    
                                    # this statement is looking for all obs values between dt_start and dt_end
                                    sql = f"SELECT ? FROM obs WHERE element = '{el_old}' AND datetime(datetime) BETWEEN '{dt_start}' AND '{dt_end}'"
                                    db_loc.exe( sql.replace("?", "COUNT(value)") )
                                     
                                    # if only 1 or 2 values exists
                                    if 1 <= n_vals <= 2:
                                        
                                        db_loc.exe( sql.replace("?", "AVG(value)") )
                                        val = db_loc.fetch1()
                                         
                                        sql_values.add( (dataset, dt_base, "", el_old, val) )
                                    

        for el_old in duration_elems:
            
            years_present = get_distinct_years()
            if not years_present: continue
            
            el_old_durs = duration_elems[el_old] # durations
            
            for dur in el_old_durs:
                
                el_old_dur  = el_old_durs[dur]
                el_new      = el_old.replace( "_", dur+"_", 1 )
                
                # if the entry for duration is empty (e.g. [10min: ~]) only copy to new element name
                if not el_old_dur:
                    # just take the values of the element with the given duration and save into new elem name
                    data = get_copy_vals(dur=dur)
                    if not data: continue
                    for i in data:
                        sql_values.add( (i[0], i[1], i[2], el_new, i[-1]) )
                    continue
                
                subdurs = el_old_dur[0] # sub-durations [12h, 6h, 3h, 1h, 30min, 10min]
                FUN     = el_old_dur[1] # function      [avg, sum, max, min]

                if len(el_old_dur) == 4:
                    el_fallback = el_old_dur[2]
                    offset      = el_old_dur[3]
                else:
                    el_fallback = None
                    offset      = 0

                for year in years_present:

                    months_present = get_distinct_months()

                    for month in months_present:

                        days_present = get_distinct_days()

                        for day in days_present:
                                   
                            break_loop = False
                            
                            #TODO how would this work with minutes as well? call get_distinct_hours already here?
                            if "h" in dur:
                                
                                dur_h = int( dur.replace("h","") )
                                
                                # first try to take the values with the given duration and save into new elem name
                                data = get_copy_vals(year, month, day, offset, dur)
                                
                                if data:
                                    for i in data:
                                        sql_values.add( (i[0], i[1], i[2], el_new, i[-1]) )
                                    # if data is complete we can continue
                                    #TODO how can we smartly omit already present data in next for loop if this is False?
                                    if len(data) == int( 24 // dur_h ):
                                        continue
                                
                            # if it does not exists we need to aggregate it using sub-durations
                            for subdur in subdurs:
                                
                                if break_loop: break
                                
                                elif "h" in subdur:

                                    dur_int     = int( dur.replace("h", "") )
                                    subdur_int  = int( subdur.replace("h", "") )
                                    
                                    dt_base     = dt( int(year), int(month), int(day) )
                                    dt_start    = dt_base - td(hours=dur_int) + td(hours=subdur_int) + td(hours=offset)
                                    dt_end      = dt_base + td(hours=offset)
                                    
                                    sql=(f"SELECT ? FROM obs WHERE element = '{el_old}' AND datetime(datetime) BETWEEN "
                                        f"'{dt_start}' AND '{dt_end}' AND ")
                                    
                                    sql_add = get_duration(FUN, dur, subdur)

                                    sql += sql_add
                                    db_loc.exe( sql.replace("?", "COUNT(value), dataset") )

                                    fetched = db_loc.fetch1()
                                    if fetched is None: continue
                                    
                                    count_vals, dataset = fetched

                                    # if we want an average make sure there are continous values for each sub-duration
                                    if FUN in {"AVG","SUM"} and int(count_vals) != int(dur_int // subdur_int):
                                        continue
                                    db_loc.exe( sql.replace("?", FUN+"(value)") )
                                    
                                    val_new = db_loc.fetch1()
                                    
                                    if val_new is not None:
                                        if verbose: print(dt_base, dur, el_new, val_new)
                                        sql_values.add( (dataset, dt_base, dur, el_new, val_new) )
                                        # break the loop when a value is first found
                                        break
                                    # only for MIN/MAX and when a fallback element is present, try again
                                    elif FUN in {"MIN","MAX"} and el_fallback:
                                            
                                        sql=(f"SELECT {FUN}(value) FROM obs WHERE element = '{el_fallback}' "
                                            f"AND datetime(datetime) BETWEEN '{dt_start}' AND "
                                            f"'{dt_end}' AND {sql_add}")
                                        
                                        db_loc.exe( sql )
                                        val_new = db_loc.fetch1()
                                        
                                        if val_new is not None:
                                            if verbose: print(dt_base, dur, el_new, val_new)
                                            sql_values.add( (dataset, dt_base, dur, el_new, val_new) )
                                            # break the loop when a value is first found
                                            break
                                    
                                elif dur in {"1h", "30min"} and "min" in subdur:
                                    
                                    for hour in get_distinct_hours():
                                            
                                        if dur == "1h": dur_int = 1
                                        else:           dur_int = 30
                                        subdur_int              = int( subdur.replace("min", "") )
                                        
                                        dt_end = dt( int(year), int(month), int(day), int(hour) )
                                        
                                        if dur == "1h":
                                            dt_start = dt_end - td(hours=dur_int) + td(minutes=subdur_int)
                                        else:
                                            dt_start = dt_end - td(minutes=dur_int) + td(minutes=subdur_int)
                                        
                                        #TODO use IN instead of BETWEEN to be more explicit and failsave? slower?
                                        sql=(f"SELECT ? FROM obs WHERE element = '{el_old}' AND datetime(datetime) BETWEEN "
                                            f"'{dt_start}' AND '{dt_end}' AND duration='{subdur}'")
                                        db_loc.exe( sql.replace("?", "COUNT(value), dataset") )
                                        
                                        fetched = db_loc.fetch1()
                                        if fetched is None: continue
                                        
                                        count_vals, dataset = fetched
                                        
                                        # convert 1h into 60mins
                                        if dur == "1h": dur_int *= 60
                                        
                                        # if we want an average make sure there are continous values for each sub-duration
                                        if FUN in {"AVG","SUM"} and int(count_vals) != int(dur_int // subdur_int):
                                            continue
                                        
                                        db_loc.exe( sql.replace("?", FUN+"(value)") )
                                        
                                        val_new = db_loc.fetch1()
                                        if val_new is not None:
                                            if verbose: print(dt_end, dur, el_new, val_new)
                                            sql_values.add( (dataset, dt_end, dur, el_new, val_new) )
                                            # break the loop when a value is first found
                                            # and break the outer loop as well
                                            break_loop = True
                                            break
                                
                                else: # if dur not in {"1h", "30"} or "h" in subdur
                                    dur_num = float( dur.replace("h", "") )
                                    if "h" in subdur:
                                        subdur_num = float( subdur.replace("h", "") )
                                    else:
                                        subdur_num = float( subdur.replace("min", "") )
                                    
                                    dt_base     = dt( int(year), int(month), int(day) )
                                    dt_start    = dt_base - td(hours=dur_int) + td(hours=subdur_num) + td(hours=offset)
                                    dt_end      = dt_base + td(hours=offset)
                                    
                                    #TODO use IN instead of BETWEEN to be more explicit and failsave? slower?
                                    sql=(f"SELECT ? FROM obs WHERE element = '{el_old}' AND datetime(datetime) BETWEEN "
                                        f"'{dt_start}' AND '{dt_end}' AND ")
                                    
                                    sql_add = get_duration(FUN, dur, subdur)
                                    
                                    sql += sql_add
                                    db_loc.exe( sql.replace("?", "COUNT(value), dataset") )
                                   
                                    fetched = db_loc.fetch1()
                                    if fetched is None: continue

                                    # if we want an average make sure there are continous values for each sub-duration
                                    if FUN in {"AVG","SUM"} and int(fetched[0]) != int(dur_num // subdur_num):
                                        continue
                                    db_loc.exe( sql.replace("?", FUN+"(value)") )
                                    
                                    val_new = db_loc.fetch1()
                                    
                                    if val_new is not None:
                                        if verbose: print(dt_end, dur, el_new, val_new)
                                        sql_values.add( (dataset, dt_end, dur, el_new, val_new) )
                                        # break the loop when a value is first found
                                        break
                                    # only for MIN/MAX and when a fallback element is present, try again
                                    elif FUN in {"MIN","MAX"} and el_fallback:

                                        sql=(f"SELECT {FUN}(value) FROM obs WHERE element = '{el_fallback}' "
                                            f"AND datetime(datetime) BETWEEN '{dt_start}' AND "
                                            f"'{dt_end}' AND {sql_add}")
                                        
                                        db_loc.exe( sql )
                                        val_new = db_loc.fetch1()
                                        
                                        if val_new is not None:
                                            if verbose: print(dt_base, dur, el_new, val_new)
                                            sql_values.add( (dataset, dt_base, dur, el_new, val_new) )
                                            # break the loop when a value is first found
                                            break
                
        if verbose: print(sql_values) 
        db_loc.exemany(SQL, sql_values)
        db_loc.close(commit=True)
         
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Aggregate observations over different time periods (durations)"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","u")
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
    output          = cf.script["output"] + "/" + mode
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]
    update          = cf.script["update"]
    aggregat_elems  = cf.script["aggregat_elems"]
    aggregat_elems  = gf.read_yaml(aggregat_elems, file_dir=cf.config_dir)
    duration_elems  = aggregat_elems["duration"]
    instant_elems   = aggregat_elems["instant"]
    sql_in_elems    = dc.sql_in( duration_elems )

    #obs             = ObsClass( cf, source, stage="forge" )
    db              = dc( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
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
            p = mp.Process(target=aggregate_obs, args=(station_group,update))
            p.start()

    else: aggregate_obs(stations, update)
