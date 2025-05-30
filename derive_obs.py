#!/usr/bin/env python
import os, sqlite3
from copy import copy
from collections import defaultdict
import global_functions as gf
import global_variables as gv
import sql_factories as sf
from datetime import datetime as dt
from database import DatabaseClass as dc
from config import ConfigClass as cc
from obs import ObsClass as oc


def derive_obs(stations):
    
    def correct_duration_9z():
        # in DWD data we need to correct the duration for 9z Tmin/Tmax obs to 15h
        sql = ("UPDATE OR IGNORE obs SET duration='15h' WHERE element IN('TMAX_2m_syn',"
            "'TMIN_2m_syn','TMIN_5cm_syn') AND strftime('%H', datetime) = '09' AND dataset "
            "IN('test','DWD','dwd_germany')")
        try:    db_loc.exe(sql)
        except: pass
        else:   db_loc.commit()
    
    #TODO structure this function but using more subroutines like the one above
    
    def derive_CL():
        pass

    def derive_CA_and_CB():
        pass

    def derive_TR_elements():
        pass

    def get_baro_height():
        pass

    def derive_qff_and_qnh():
        pass
    
    def derive_dewpoint():
        pass
    
    def derive_metwatch():
        pass

    for loc in stations:
        print(loc)
        sql_values = set()
        
        db_file = obs.get_station_db_path(loc)
        #db_file = f"{output}/{mode}/forge/{loc[0]}/{loc}.db"
        try: db_loc = dc( db_file, row_factory=sf.list_row )
        except Exception as e:
            gf.print_trace(e)
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue
       
        if not dt_30min: correct_duration_9z()
        
        """
        sql1=f"SELECT dataset,datetime,duration,element,value FROM obs WHERE element = '%s'{dt_30min}"
        sql2=f"INSERT INTO obs (dataset,datetime,duration,element,value) VALUES(?,?,?,?,?) ON CONFLICT DO {on_conflict}"
        
        found = False
        
        for replace in replacements:
            print(replace)
            replace_order = replacements[replace].split(",")
            for i in range(len(replace_order)):
                #if found: break
                db_loc.con.row_factory = sf.list_row
                db_loc.exe(sql1 % replace_order[i])
                #print(sql1 % replace_order[i])
                data = db_loc.fetch()
                #if data: found = True
                for j in data:
                    print(j)
                    j[2] = replace
                    print(j)
                    sql_values.add( tuple(j) )

        print(sql_values)
        db_loc.exemany(sql2, sql_values)
        """    
        
        #TODO remove this section if the approach below works better + faster
        sql = ("SELECT dataset,datetime,element,round(value) from obs WHERE element IN "
            "('CA{i}_2m_syn', 'CB{i}_2m_syn'){dt_30min} ORDER BY datetime asc, element desc")
        
        # https://discourse.techart.online/t/python-group-nested-list-by-first-element/3637

        sql_values = set()

        for i in range(1,5):
            #print(sql.format(i=i))
            db_loc.exe(sql.format(i=i, dt_30min=dt_30min))
            data = db_loc.fetch()
            
            CL              = defaultdict(str)
            cloud_covers    = set()

            for j in data:
                dataset     = j[0]
                datetime    = j[1]
                if len(CL[j[-1]]) == 0 and j[2] == f"CA{i}_2m_syn" and j[-1] not in cloud_covers:
                    CL[j[-1]]    += str(int(j[-1]))
                    cloud_covers.add(j[-1])
                elif len(CL[j[-1]]) == 1 and j[2] == f"CB{i}_2m_syn" and j[-1] in cloud_covers:
                    CB_in_30m   = int(j[-1]/30)
                    CL[j[-1]]    += str(CB_in_30m).rjust(3,"0")

            CL = dict(CL)

            for k in CL:
                if len(CL[k]) == 1:
                    CL[k] += "///"
                sql_values.add( (dataset, datetime, k, f"CL{i}_2m_syn", CL[k]) )

        # duration is always 1s for cloud observations
        sql_insert = (f"INSERT INTO obs (dataset,datetime,element,value,duration) "
            f"VALUES(?,?,?,?,'1s') ON CONFLICT DO {on_conflict}")
        try:    db_loc.exemany(sql_insert, sql_values)
        except: pass

        # https://stackoverflow.com/a/49975954
        sql_delete = "DELETE FROM obs WHERE length(value) > 4 AND element LIKE 'CL%_2m_syn'"
        try:    db_loc.exe(sql_delete)
        except: pass
       
        # derive cloud amounts [CA?_2m_syn] and cloud bases in the 4 levels [CB?_2m_syn]
        # from cloud levels [CL?_2m_syn] (usually provided by metwatch CSVs)
        sql_select = (f"SELECT dataset,element,value FROM obs WHERE element LIKE "
            f"'CL%_2m_syn'{dt_30min}")
        db_loc.exe(sql_select)
        
        data        = db_loc.fetch()
        sql_values  = set()

        for i in data:
            dataset     = i[0]
            datetime    = i[1]
            element     = i[2]
            element_CAx = element[:2] + "A" + element[3:]
            element_CBx = element[:2] + "B" + element[3:]
            value_CAx   = value[0]
            value_CBx   = value[1:]

            sql_values.add( (dataset, datetime, element_CAx, value_CAx) )
            sql_values.add( (dataset, datetime, element_CBx, value_CBx) )
        
        #try:    db_loc.exemany(sql_insert, sql_values)
        #except: pass
        
        # do the opposite of the above:
        # derive cloud levels [CL?_2m_syn] from cloud amounts [CA?_2m_syn] and bases [CB?_2m_syn]
        sql = (f"SELECT dataset,datetime,element,value FROM obs WHERE element REGEXP "
            f"'(CA._2m_syn|CB._2m_syn)'{dt_30min} ORDER BY datetime asc, element desc")
        db_loc.row_factory = sf.dict_row
        db_loc.exe(sql)
          
        # set row factory to tuple
        db_loc.row_factory = sf.tuple_row
        
        data = db_loc.fetch()
        if verbose: print("CL")
        datetime_prev, element_prev, value_prev = None, None, None
        
        for i in data:
            dataset     = i[0]
            datetime    = i[1]
            element     = i[2]
            value       = str(int(float(i[3])))
            if verbose: print(dataset, datetime, element, value)
            
            if datetime==datetime_prev and element[2]==element_prev[2] and element != element_prev:
                try:    value_CAx = str(int(round(float(value_prev))))
                except: continue
                try:    value_CBx = str(int(round(float(value)))).rjust(3,"0")
                except: continue
                value_CLx = value_CAx + value_CBx
                if len(value_CLx) == 4:
                    if verbose:
                        print(f"CL{element[2:]}")
                        print(value_CLx)
                    sql_values.add( (dataset, datetime, f"CL{element[2:]}", value_CLx) )
            
            datetime_prev   = copy(datetime)
            element_prev    = copy(element)
            value_prev      = copy(value)
        
        try:    db_loc.exemany(sql_insert, sql_values)
        except: pass
        
        # reset row factory to default
        db_loc.row_factory = sf.default_row
        
        """
        # this is needed when deriving obs imported from metwatch csv files (import_metwatch.py)
        
        # do this for all metwatch elements which can be linked to a TR
        # derive [element, TR] from element and TR
        # derive [PRATE_1m_syn, TR] from PRATE_1m_syn and corresponding TR

        # get all datetime where TR and obs elements are present and have a NOT NULL value
        sql = (f"SELECT DISTINCT datetime FROM obs WHERE duration = 'TR' AND "
            f"value IS NOT NULL{dt_30min} INTERSECT SELECT DISTINCT datetime FROM obs WHERE "
            f"element = 'TR' AND value IS NOT NULL{dt_30min}")
        db_loc.row_factory = sf.tuple_row
        db_loc.exe(sql)
        # reset row factory
        db_loc.row_factory = sf.default_row

        #sql_insert  = (f"INSERT INTO obs (dataset,datetime,element,value,duration) "
        #    f"VALUES(?,?,'PRATE_1m_syn',?,?) ON CONFLICT DO {on_conflict}")
        sql_insert  = (f"INSERT INTO obs (dataset,datetime,element,value,duration) "
            f"VALUES(?,?,?,?,?) ON CONFLICT DO {on_conflict}")
        prate_vals  = set()
        
        # get result of SELECT statement as a polars LazyFrame
        db_loc.row_factory = sf.polars_lf_row
        # get result of SELECT statement as a polars DataFrame
        db_loc.row_factory = sf.polars_df_row
        
        # the TR element also has duration='TR' so we can query all needed data with only one SELECT
        sql = (f"SELECT dataset,datetime,element,value FROM obs WHERE duration = 'TR'{dt_30min}")
        #db_loc.exe(sql)
        
        cur     = db_loc.cur
        # we will retrieve the data directly as a polars DataFrame
        obs_df  = db_loc.fetch_polars_df(cur, sql)
        #obs_lf  = db_loc.fetch_polars_lf(cur, sql)

        print("obs_df")
        print(obs_df)
         
        #print("obs_lf")
        #print(obs_lf)
        
        # if DataFrame has at least 2 columns (we need TR + one TR-dependent element)
        if obs_df.width > 1:
            print( db_loc.desc, cur.description )
            obs_df.columns = cur.description # ["dataset","datetime","element","value"]
            
            # in all distinct datetimes, connect element with duration provided by TR (dur=f"{TR}h")
            obs_select      = obs_df.select(["datetime","element"]).unique()
            obs_unique_dt   = obs_df.unique(subset=["datetime"])
            
            print("SELECT, UNIQUE")
            print(obs_select, obs_unique_dt)
            
            for name, data in obs_df.group_by(["datetime"]):
                print(name, data)
                 
                value_TR = data.select(["TR"])
                 
            
            for values in obs_dict:
                
                dataset, element, value = values
                
                if element == "TR": duration    = str(value) + "h"
                # if we had element="TR" already
                elif duration:
                    value_obs   = copy(value)
                    # add the observation to the sql values set
                    prate_vals.add( (dataset, datetime, element, value_obs, duration) )
            import sys
            sys.exit()
        
        # reset row factory
        db_loc.row_factory = sf.default_row
        db_loc.exemany(sql_insert, prate_vals)
        continue
        """
        sql_insert  = (f"INSERT INTO obs (dataset,datetime,element,value,duration) "
            f"VALUES(?,?,?,?,'1s') ON CONFLICT DO {on_conflict}")

        # try to calculate QFF+QNH if no reduced pressure is present in obs
        # and we have barometer height instead
        db = dc( config=cf.database, ro=1 )
        
        # we should actually prefer the barometer elevation over general elevation 
        # because they can differ a lot
        try:
            baro_height = db.get_station_baro_elev(loc)
        except sqlite3.OperationalError:
            baro_height = None
        
        #TODO when baro_elev is implemented in amalthea/main main.db: incomment this line
        # and delete try/except above
        #baro_height = db.get_station_baro_elev(loc)
        
        if baro_height is None:
            baro_height     = db.get_station_elevation(loc)
            station_height  = copy(baro_height)
        else:
            station_height  = copy(baro_height)

        db.close()
        

        # derive reduced pressure (QFF and QNH) if only station pressure was reported
         
        if baro_height is not None:
            
            db  = dc( config=cf.database, ro=1 )
            lat = db.get_station_latitude(loc)
            db.close()
            
            # first get all datetimes where there is no PRMSL recorded but PRES (30min values only)
            # SELECT NULL or EMPTY: https://stackoverflow.com/questions/3620828/sqlite-select-where-empty
            sql = (f"SELECT DISTINCT datetime FROM obs WHERE  element LIKE 'PRES_0m_syn' AND value"
                f" IS NOT NULL{dt_30min} INTERSECT SELECT DISTINCT datetime FROM obs WHERE "
                f"element LIKE 'PRMSL_ms_%' AND IFNULL(value, '') = ''{dt_30min}")
                #AND value IS NULL OR value = ''
            db_loc.exe(sql)
            
            datetimes   = set( i[0] for i in db_loc.fetch() )
            sql_insert  = (f"INSERT INTO obs (dataset,datetime,element,value,duration) "
                f"VALUES(?,?,?,?,'1s') ON CONFLICT DO {on_conflict}")
            prmsl_vals  = set()

            # try calculate PRMSL for all datetimes where only PRES is available
            sql = (f"SELECT dataset,datetime,value FROM obs WHERE element = 'PRES_0m_syn' "
                f"AND value IS NOT NULL{dt_30min}")
            db_loc.exe(sql)

            for row in db_loc.fetch():
                if debug: print("ROW", row)
                dataset     = row[0]
                datetime    = row[1]
                value_PRES  = float(row[2])
                
                # we prefer to use qff, so try to get all needed elements for it 
                sql = (f"SELECT value FROM obs WHERE element IN ('TMP_2m_syn', 'DPT_2m_syn', "
                    f"'RH_2m_syn') AND datetime = '{datetime}' ORDER BY element")
                db_loc.exe(sql)
                
                values = db_loc.fetch()
                
                if len(values) == 3:
                    value_DPT   = float(values[0][0])
                    value_RH    = float(values[1][0])
                    value_TMP   = float(values[2][0])
                else: continue

                #if station and baro height differ: calculate QFE
                #if station_height != baro_height:
                #   qfe = gf.qfe(value_PRES, baro_height-station_height)
                
                if debug: print("VALUES DPT RH TPP", values)

                if value_PRES is not None and value_TMP is not None and baro_height <= 350:
                    pr_qnh = gf.qnh( value_PRES, baro_height, value_TMP )
                    prmsl_vals.add( (dataset, datetime, "PRMSL_ms_met", pr_qnh) )
                    
                    # if dewpoint or relative humidity present: use DWD reduction method [page 106]
                    #https://www.dwd.de/DE/leistungen/pbfb_verlag_vub/pdf_einzelbaende/vub_2_binaer_barrierefrei.pdf?__blob=publicationFile&v=4
                    if value_DPT is not None or value_RH is not None and baro_height < 750:
                        
                        if value_RH is None:
                            value_RH = gf.dpt2rh(value_DPT, value_TMP)
                        
                        # relative humidity needs to be between 0 and 100 percent
                        if 0 <= value_RH <= 100:
                            if debug: print("PPP, h, TMP, RH")
                            if debug: print(value_PRES, baro_height, value_TMP, value_RH )
                            pr_qff  = gf.qff_dwd( value_PRES, baro_height, value_TMP, value_RH )
                            prmsl_vals.add( (dataset, datetime, "PRMSL_ms_syn", pr_qff) )
                
            db_loc.exemany(sql_insert, prmsl_vals)
            

            # calculate derivation of dewpoint temperature here, add unit conversions...
            # first get all datetimes where no dewpoint is present but we have RH and TMP recorded
            sql = (f"SELECT datetime FROM obs WHERE element = 'RH_2m_syn' AND value IS NOT NULL"
                f"{dt_30min} INTERSECT SELECT datetime FROM obs WHERE element = 'TMP_2m_syn' AND "
                f"value IS NOT NULL{dt_30min} INTERSECT SELECT datetime FROM obs WHERE "
                f"element = 'DPT_2m_syn' AND IFNULL(value, '') = ''{dt_30min}")
            db_loc.exe(sql)
            
            sql_insert  = (f"INSERT INTO obs (dataset,datetime,element,value,duration) VALUES"
                f"(?,?,'DPT_2m_syn',?,'1s') ON CONFLICT DO {on_conflict}")
            dpt_vals    = set()
            
            for datetime in db_loc.fetch():
                datetime = datetime[0]
                sql = (f"SELECT dataset,value FROM obs WHERE element IN ('RH_2m_syn','TMP_2m_syn') "
                    f"AND datetime = '{datetime}' AND value IS NOT NULL ORDER BY element")
                db_loc.exe(sql)
                
                values      = db_loc.fetch()[0]
                if len(values) < 3: continue
                if debug: print("VALUES", values)
                dataset     = values[0]
                value_RH    = float(values[1])
                
                # relative humidity needs to be between 0 and 100 percent
                if 0 <= value_RH <= 100:
                    value_TMP   = float(values[2])
                    value_DPT   = gf.rh2dpt(values_RH, value_TMP)
                     
                    # dewpoint temperature needs to be smaller than OR equal to temperature
                    if value_DPT <= value_TMP:
                        dpt_vals.add( (dataset, datetime, "DPT_2m_syn", value_DPT) )
                
            db_loc.exemany(sql_insert, dpt_vals)
            

            # correct durations for W1W2
            W1W2_cors = set()
            sql = (f"SELECT datetime, strftime('%H', datetime) as hour, element, value FROM obs "
                    f"WHERE element LIKE 'W__2m_syn' AND value IS NOT NULL AND "
                    f"strftime('%M', datetime) = '00' AND duration NOT LIKE '_h'")
            db_loc.exe(sql)
            
            for datetime, hour, element, value in db_loc.fetch():
                duration = W1W2_durations[int(hour)]
                if verbose: print(datetime, duration, element, value)
                W1W2_cors.add( (dataset, datetime, duration, element, value) )
            
            sql_insert = (f"INSERT INTO obs (dataset,datetime,element,value,duration) VALUES"
                f"(?,?,?,?,?) ON CONFLICT DO {on_conflict}")
            db_loc.exemany(sql_insert, W1W2_cors)
                
                
            ##TODO MEDIUM priority, could be useful for some sources
            
            #TODO derive total sunshine duration in min from % (using astral package; see wetterturnier)
            #import astral 
            
            #TODO derive 2 digit (SYNOP) ww code from METAR significant weather code?
            
            #TODO derive 2 digit (SYNOP) ww code from 3 digit (BUFR) ww code?

            #TODO derive 1 digit (SYNOP) W1W2 code from 2 digit (BUFR) W1W2 code?

            #TODO derive 1 digit (SYNOP) ground state code from 2 digit (BUFR) ground state code?
            
            
            ##TODO LOW priority, not really needed at the moment
            
            #TODO take 5m wind as 10m wind if 10m wind not present (are they compareble???)
            
            #TODO derive wind direction from U and V components
             
            #TODO derive precipitation amount from duration and intensity (only if missing)
            # -> then it might be necessary to aggregate again afterwards...
            
        #db_loc.commit() 
        db_loc.close(commit=True)
        
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Derive obs elements from other parameters"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","A","u")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, clusters=False)
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
    #replacements    = cf.script["replacements"]
    #combinations    = cf.script["combinations"]
    update          = cf.script["update"]
    sources         = cf.args.source

    if update:
        on_conflict = "UPDATE SET value=excluded.value"
    else:
        on_conflict = "NOTHING"
    
    if cf.script["aggregated"]:
        dt_30min = " AND strftime('%M', datetime) IN ('00','30')"
    else: dt_30min = ""
    
    #TODO implement WHERE dataset='{source}' or AND dataset='{source}' in all SELECT statements
    if len(sources) > 0:
        sql             = dc.sql_equal_or_in(sources)
        and_dataset     = f" AND dataset {sql}"
        where_dataset   = f" WHERE dataset {sql}"
    else:
        and_dataset, where_dataset = "", ""
    
    obs             = oc( cf, mode=mode, stage="forge", verbose=verbose )
    db              = dc( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)
    
    # get SYNOP codes conversion dictionary to decode SYNOP codes
    #synop_codes     = gf.read_yaml("codes/synop", file_dir=cf.config_dir)
    # get METAR codes conversion dictionary to decode METAR codes
    #metar_codes     = gf.read_yaml("codes/metar", file_dir=cf.config_dir)
   
    # W1W2 reference period is 6h for 0,6,12,18z, 3h for 3,9,15,21z and 1h for the other hours
    W1W2_durations = { 0: 6, 1: 1, 2: 1, 3: 3, 4: 1, 5: 1, 6: 6, 7: 1, 8:1, 9: 3, 10: 1, 11: 1,
            12: 6, 13:1, 14: 1, 15: 3, 16: 1, 17: 1, 18: 6, 19: 1, 20:1, 21: 3, 22: 1, 23: 1 }
    
    if processes: # number of processes
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, processes)
        station_groups = np.array_split(stations, processes)

        for station_group in station_groups:
            p = mp.Process(target=derive_obs, args=(station_group,))
            p.start()

    else: derive_obs(stations)
    
    finished_str = gf.get_finished_str(script_name)
    if verbose: print(finished_str)
