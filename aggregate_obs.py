import sys
from datetime import datetime as dt, timedelta as td
from database import database
import global_functions as gf

config          = gf.read_yaml( "config.yaml" )
db_settings     = config["database"]["settings"]
config_script   = config["scripts"][sys.argv[0]]
verbose         = config_script["verbose"]
traceback       = config_script["traceback"]

db              = database( config["database"]["db_file"],verbose=0,traceback=1,settings=db_settings )

cluster         = config_script["station_cluster"]
stations        = db.get_stations( cluster ); db.close(commit=False)
params          = config_script["params"]

#TODO implement source option! for now, just stick with test
src = "test"

def aggregate_stations(stations):
    def get_distinct_months(year):
        db_stat.exe((f"SELECT DISTINCT strftime('%m', datetime) FROM obs WHERE element = '{p_old}' "
                        f"AND strftime('%Y', datetime) = '{year}'"))
        return db_stat.fetch()


    def get_distinct_days(year, monrh):
        db_stat.exe((f"SELECT DISTINCT strftime('%d', datetime) FROM obs WHERE element='{p_old}' AND "
                        f"strftime('%Y', datetime) = '{year}' AND strftime('%m', datetime) = '{month}'"))
        return db_stat.fetch()


    def get_distinct_hours(year, month, day):
        db_stat.exe((f"SELECT DISTINCT strftime('%H', datetime) FROM obs WHERE element='{p_old}' AND strftime('%Y', "
            f"datetime) = '{year}' AND strftime('%m', datetime) = '{month}' AND strftime('%d', datetime) = '{day}'"))
        return db_stat.fetch()

    for loc in stations:
        
        sql_values = set()

        try: db_stat = database( f"/home/juri/data/stations/raw/{loc[0]}/{loc}.db", verbose=True, traceback=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue
       
        try: db_stat.exe( f"SELECT DISTINCT strftime('%Y', datetime) FROM obs" )
        except: continue
        years_present = db_stat.fetch()

        for p_new in params:

            dur = params[p_new][0] # duration
            FUN = params[p_new][1] # function

            p_old = p_new.replace( dur + "_", "" )
     
            if not FUN: # dur == "10min"
                # just take the values of the parameter with the given duration and save into to the new param name
                db_stat.exe( f"SELECT * FROM obs WHERE element = '{p_old}' AND duration = '{dur}'" )
                data = db_stat.fetch()
                if not data: continue
                for i in data:
                    sql_values.add( (i[0], i[1], i[2], p_new, i[4]) )

            # get parameter values with a lower time dimensions (h->min, 24h->1h) and aggregate over duration
            elif dur == "1h":
                # first get all obs which already have 1h duration and copy them with the new parameter name
                for hh in range(0, 24):
                    hh = str(hh).rjust(2,"0")
                    db_stat.exe((f"SELECT * FROM obs WHERE element = '{p_old}' AND duration = '{dur}' "
                                    f"AND strftime('%H', datetime) = '{hh}' AND strftime('%M', datetime) = '00'"))
                    data = db_stat.fetch()
                    if data:
                        for i in data:
                            sql_values.add( (i[0], i[1], i[2], p_new, i[4]) )

                # then, get all 10min (30min?) values and aggregate them with the respective function (min,max,mean)
                #TODO
                for year in years_present:
                    #TODO smartly handle data gaps, use only years, months and days where data is actually present
                    
                    # for we keep it simple and just look for the min and max datetime values and loop over the range
                    months_present = get_distinct_months(year)

                    for month in months_present:
                        
                        mm = month.rjust(2,"0")

                        days_present = get_distinct_days(year, month)
                        
                        for day in days_present:
                            
                            dd = day.rjust(2,"0")
                            
                            hours_present = get_distinct_hours(year, month, day)

                            for hour in hours_present:
                                
                                hh = hour.rjust(2,"0")

                                # first try 30 min values
                                sql=(f"SELECT ? FROM obs WHERE element = '{p_old}' AND duration = '30min' AND "
                                     f"strftime('%Y',datetime) = '{year}' AND strftime('%m',datetime) = '{mm}' AND "
                                     f"strftime('%d', datetime) = '{dd}' AND strftime('%H', datetime) = '{hh}'")

                                #TODO add dataset to make sure the values are really unique and max of count() is 6
                                db_stat.exe( sql.replace("?", "COUNT(value)") )

                                # if we want an average make sure there are exactly six 10min values
                                if FUN in ("AVG","SUM") and db_stat.fetch1() != 2:
                                    pass
                                else:
                                    db_stat.exe( sql.replace("?", FUN+"(value)") )
                                    v_new = db_stat.fetch1()

                                    if v_new is not None:
                                        dt_new = dt( int(year), int(month), int(day), int(hour) )

                                        if verbose: print(src, dt_new, dur, p_new, v_new)
                                        sql_values.add( (src, dt_new, dur, p_new, v_new) )
                                        continue

                                # try 10 min values
                                sql=(f"SELECT ? FROM obs WHERE element = '{p_old}' AND duration = '10min' AND "
                                     f"strftime('%Y',datetime) = '{year}' AND strftime('%m',datetime) = '{mm}' AND "
                                     f"strftime('%d', datetime) = '{dd}' AND strftime('%H', datetime) = '{hh}'")
                                
                                #TODO add dataset to make sure the values are really unique and max of count() is 6
                                db_stat.exe( sql.replace("?", "COUNT(value)") )
                               
                                # if we want an average make sure there are exactly six 10min values
                                if FUN in ("AVG","SUM") and db_stat.fetch1() != 6:
                                    continue
                               
                                db_stat.exe( sql.replace("?", FUN+"(value)") )
                                v_new = db_stat.fetch1()
                                
                                if v_new is not None:
                                    dt_new = dt( int(year), int(month), int(day), int(hour) )
                                    
                                    if verbose: print(src, dt_new, dur, p_new, v_new)
                                    sql_values.add( (src, dt_new, dur, p_new, v_new) )

            
            else: # dur in [12h, 24h]
                # get all 24h, 12h, 6h, 3h, 1h (30min, 10min?) values and aggregate them with the resp. function
                # start with 24h, then try the shorter timespans
                
                offset = params[p_new][2]
                timespan = int(dur[:2]) # 12 or 24

                # 24h
                db_stat.exe( f"SELECT * FROM obs WHERE element = '{p_old}' AND duration = '{dur}'" )
                data = db_stat.fetch()
                if not data: pass
                else:
                    for i in data:
                        sql_values.add( (i[0], i[1], i[2], p_new, i[4]) )
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
                                
                                db_stat.exe( sql.replace("?", "COUNT(value)") )

                                # if we want an average make sure there are continous values for each sub-duration
                                if FUN in ("AVG","SUM") and int(db_stat.fetch1()) != int(24 // subdur):
                                    continue
                                db_stat.exe( sql.replace("?", FUN+"(value)") )
                                v_new = db_stat.fetch1()

                                if v_new is not None:
                                    if verbose: print(src, dt_ts, dur, p_new, v_new)
                                    sql_values.add( (src, dt_ts, dur, p_new, v_new) )
                                    break

        sql=(f"INSERT INTO obs VALUES(?,?,?,?,?) ON CONFLICT DO UPDATE SET value = excluded.value")
        db_stat.exemany(sql, sql_values)
        db_stat.close(commit=True)


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        # number of processes
        npcs = int(sys.argv[1])
        import multiprocessing as mp
        from random import shuffle

        n_stations = len(stations)
        station_groups = set()
        stations = list(stations)
        shuffle(stations)

        for i in range(0, n_stations-1, npcs):
            station_groups.add( tuple( stations[i:i+int(n_stations/npcs)] ) )

        for station_group in station_groups:
            p = mp.Process(target=aggregate_stations, args=(station_group,))
            p.start()

    else: aggregate_stations(stations)
