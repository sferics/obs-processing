#!/usr/bin/env python
import os
import sys
from datetime import datetime as dt, timedelta as td
from obs import ObsClass
from database import DatabaseClass as dc
from config import ConfigClass
import global_functions as gf
import global_variables as gv


def aggregate_obs(stations):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    def get_distinct_months(year):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe((f"SELECT DISTINCT strftime('%m', datetime) FROM obs WHERE element = '{p_old}' "
                        f"AND strftime('%Y', datetime) = '{year}'"))
        return db_loc.fetch()


    def get_distinct_days(year, monrh):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe((f"SELECT DISTINCT strftime('%d', datetime) FROM obs WHERE element='{p_old}' AND "
                        f"strftime('%Y', datetime) = '{year}' AND strftime('%m', datetime) = '{month}'"))
        return db_loc.fetch()


    def get_distinct_hours(year, month, day):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        db_loc.exe((f"SELECT DISTINCT strftime('%H', datetime) FROM obs WHERE element='{p_old}' AND strftime('%Y', "
            f"datetime) = '{year}' AND strftime('%m', datetime) = '{month}' AND strftime('%d', datetime) = '{day}'"))
        return db_loc.fetch()


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
      
        #TODO remove this temporary fix!
        # delete all aggregated paramters before calculation them again
        sql = f"DELETE FROM obs WHERE element IN{aggreg_elements}"
        try:    db_loc.exe( sql )
        except Exception as e:
            print(e)
            gf.print_trace(e)

        try: db_loc.exe( f"SELECT DISTINCT strftime('%Y', datetime) FROM obs" )
        except: continue
        years_present = db_loc.fetch()

        for p_new in params:

            dur = params[p_new][0] # duration
            FUN = params[p_new][1] # function

            p_old = p_new.replace( dur + "_", "" )
     
            if not FUN: # and dur == "10min"
                # just take the values of the parameter with the given duration and save into to the new param name
                db_loc.exe( f"SELECT * FROM obs WHERE element = '{p_old}' AND duration = '{dur}'" )
                data = db_loc.fetch()
                if not data: continue
                for i in data:
                    sql_values.add( (i[0], i[1], p_new, i[-1]) )

            # get parameter values with a lower time dimensions (h->min, 24h->1h) and aggregate over duration
            elif dur == "1h":

                # first get all obs which already have 1h duration and copy them with the new parameter name
                for hh in range(0, 24):
                    hh = str(hh).rjust(2,"0")
                    db_loc.exe((f"SELECT * FROM obs WHERE element = '{p_old}' AND duration = '{dur}' "
                                    f"AND strftime('%H', datetime) = '{hh}' AND strftime('%M', datetime) = '00'"))
                    data = db_loc.fetch()
                    if data:
                        for i in data: sql_values.add( (i[0], i[1], p_new, i[-1]) )
                        # if we found a 1 h duration value, continue with next hour, no need to check 30 and 10 min
                        continue

                # then, get all 30min and values and aggregate them with the respective function (min,max,avg)
                for year in years_present:
                    
                    # smartly handle data gaps, use only years, months and days where data is actually present
                    
                    months_present = get_distinct_months(year)

                    for month in months_present:
                        
                        mm = month.rjust(2,"0")

                        days_present = get_distinct_days(year, month)
                        
                        for day in days_present:
                            
                            dd = day.rjust(2,"0")
                            
                            hours_present = get_distinct_hours(year, month, day)

                            for hour in hours_present:
                                
                                hh = int(hour.rjust(2,"0"))

                                # first try 30 min values
                                sql=(f"SELECT ? FROM obs WHERE element = '{p_old}' AND duration = '30min' AND "
                                     f"strftime('%Y', datetime) = '{year}' AND strftime('%m', datetime) = '{mm}' AND "
                                     f"strftime('%d', datetime) = '{dd}' AND ( ( strftime('%H', datetime) = '{hh}' AND "
                                     f"strftime('%M', datetime) = '30' ) OR ( strftime('%H', datetime) = '{hh+1}' AND "
                                     f"strftime('%M', datetime) = '00' ) )")

                                # add dataset to make sure the values are really unique and max of count() is 2
                                try: db_loc.exe( sql.replace("?", "COUNT(value)") )
                                except Exception as e:
                                    print(e)
                                    gf.print_trace(e)
                                    continue

                                # if we want an average make sure there are exactly two 10min values
                                if FUN in {"avg","sum"} and db_loc.fetch1() != 2:
                                    # else continue after else with the 10 min values
                                    pass
                                else:
                                    try: db_loc.exe( sql.replace("?", FUN+"(value)") )
                                    except Exception as e:
                                        print(e)
                                        gf.print_trace(e)
                                        continue

                                    v_new = db_loc.fetch1()

                                    if v_new is not None:
                                        dt_new = dt( int(year), int(month), int(day), int(hour) )

                                        if verbose: print(dt_new, dur, p_new, v_new)
                                        sql_values.add( (dt_new, dur, p_new, v_new) )
                                        continue

                                # try 10 min values
                                sql=(f"SELECT ? FROM obs WHERE element = '{p_old}' AND duration = '10min' AND "
                                     f"strftime('%Y', datetime) = '{year}' AND strftime('%m', datetime) = '{mm}' AND "
                                     f"strftime('%d', datetime) = '{dd}' AND ( ( strftime('%H', datetime) = '{hh}' AND "
                                     f"strftime('%M', datetime) IN ('10', '20', '30', '40', '50') ) OR ( "
                                     f"strftime('%H', datetime) = '{hh+1}' AND strftime('%M',datetime) = '00') )")
                                
                                #TODO add dataset to make sure the values are really unique and max of count() is 6
                                try: db_loc.exe( sql.replace("?", "COUNT(value)") )
                                except Exception as e:
                                    print(e)
                                    gf.print_trace(e)
                                    continue

                                # if we want an average make sure there are exactly six 10min values
                                if FUN in {"avg","sum"} and db_loc.fetch1() != 6:
                                    continue
                               
                                try: db_loc.exe( sql.replace("?", FUN+"(value)") )
                                except Exception as e:
                                    print(e)
                                    gf.print_trace(e)
                                    continue

                                v_new = db_loc.fetch1()
                                
                                if v_new is not None:
                                    dt_new = dt( int(year), int(month), int(day), int(hour) )
                                    
                                    if verbose: print(dt_new, dur, p_new, v_new)
                                    sql_values.add( (dt_new, dur, p_new, v_new) )

            
            else: # dur in [12h, 24h]
                # get all 24h, 12h, 6h, 3h, 1h (30min, 10min?) values and aggregate them with the resp. function
                # start with 24h, then try the shorter timespans
                
                offset = params[p_new][2]
                timespan = int(dur[:2]) # 12 or 24

                # 24h
                db_loc.exe( f"SELECT * FROM obs WHERE element = '{p_old}' AND duration = '{dur}'" )
                data = db_loc.fetch()
                if not data: pass
                else:
                    for i in data:
                        sql_values.add( (i[0], i[1], p_new, i[-1]) )
                    continue 

                for year in years_present:
                    
                    months_present = get_distinct_months(year)
                    
                    for month in months_present:
                        
                        days_present = get_distinct_days(year, month)
                        
                        for day in days_present:
                            
                            dt_ts = dt( int(year), int(month), int(day), offset ) + td(hours=timespan)
                           
                            #TODO is there a possibility to use 15 or 9 hour data in- or outside of this loop?
                            for subdur in ( 12, 6, 3, 1, 0.5, 1/6 ): # aggregate 12h,6h 3h,1h,30min,10min durations
                                
                                if subdur >= 1:
                                    dt0 = dt( int(year), int(month), int(day), offset ) + td(hours=subdur)
                                    dur = str(subdur) + "h"
                                else:
                                    dt0 = dt( int(year), int(month), int(day), offset, int(subdur*60) )
                                    dur = str(subdur*60) + "min"

                                sql=(f"SELECT ? FROM obs WHERE element = '{p_old}' AND datetime(datetime) BETWEEN "
                                     f"'{dt0}' AND '{dt_ts}' AND substr(datetime,16,1)='0' AND duration='{dur}'")
                                
                                db_loc.exe( sql.replace("?", "COUNT(value)") )

                                # if we want an average make sure there are continous values for each sub-duration
                                if FUN in {"avg","sum"} and int(db_loc.fetch1()) != int(24 // subdur):
                                    continue
                                db_loc.exe( sql.replace("?", FUN+"(value)") )
                                v_new = db_loc.fetch1()

                                if v_new is not None:
                                    if verbose: print(dt_ts, dur, p_new, v_new)
                                    sql_values.add( (dt_ts, dur, p_new, v_new) )
                                    break

        sql = f"INSERT INTO obs VALUES(?,?,?,?) ON CONFLICT DO UPDATE SET value = excluded.value, duration = excluded.duration"
        db_loc.exemany(sql, sql_values)
        db_loc.close(commit=True)
    
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Aggregate observations over different time periods"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P")
    cf          = ConfigClass(script_name, pos=["source"], flags=flags, info=info, verbose=True)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"

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
    params          = cf.script["params"]

    obs             = ObsClass( cf, source, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)

    #TODO remove
    aggreg_elements = tuple(params.keys())

    if processes: # number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=aggregate_obs, args=(station_group,))
            p.start()

    else: aggregate_obs(stations)
