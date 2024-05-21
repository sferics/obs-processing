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

#TODO HIGH PRIORITY! ESSENTIAL!

  #ClNCmNChN: [CLCMCH_2m_syn,  ~, 1, 0]  # Wolkenarten in den Stockwerken                   (zB: Cu2Ac3Cs6) -> 236
  #ClNCmNChN: [CLCMCH_2m_syn,  ~, 1, 0]  # -> Wolkenmenegen in den Stockwerken              (zB: 3451, 1///)
  #NC1XXX:    [CL1_2m_syn,     ~, 1, 0]  # unterste Wolkenschicht Bedeckung/Art/Untergrenze (zB: 2ST020) -> 2020
  #NC2XXX:    [CL2_2m_syn,     ~, 1, 0]  # 2.Wolkenschicht                                  (zB: 4AC100) -> 4100
  #NC3XXX:    [CL3_2m_syn,     ~, 1, 0]  # 3.Wolkenschicht                                  (zB: 5CS300) -> 5300
  #NC4XXX:    [CL4_2m_syn,     ~, 1, 0]  # 4.Wolkenschicht                                  (zB: 2CB080) -> 2080
  #NC1XXX:    [CL?_2m_syn,     ~, 1, 0]  # Wolkenschicht Bedeckung+Untergrenze              (zB: 1015, 5300, 2080)

#TODO if no TCC_LC_syn: take TCC_ceiling_syn

#TODO derive total cloud cover per height level (CL, CM, CH)

#TODO derive cloud layers (CL_?) from cloud bases and cloud amounts

# only in station_test: first devide cloud height by 30

# CL?_2m_syn = TCC_?C_syn + CB?_2m_syn
# if no TCC_[1-3]C_syn present:
# CL1_2m_syn = CDCL_2m_syn + CB1_2m_syn
# CL2_2m_syn = CDCM_2m_syn + CB2_2m_syn
# CL3_2m_syn = CDCH_2m_syn + CB3_2m_syn

#TODO derive VIS_2m_syn from MOR_2m_syn, MOR_2m_min, MOR_2m_max, VIS_2m_min, VIS_2m_pre, VIS_2m_run, VIS_2m_sea
# if no VIS_2m_syn: VIS_2m_syn = MOR_2m_syn
# [if no VIS_2m_syn and no MOR_2m_syn: VIS_2m_min, MOR_2m_min, VIS_2m_pre, VIS_2m_run, VIS_2m_sea, MOR_2m_max (priorities)]

# if key is not found, try to take replacements[key], else ignore

def derive_obs(stations):
    
    for loc in stations:

        sql_values = set()
        
        db_file = f"{output}/forge/{loc[0]}/{loc}.db"
        try: db_loc = dc( db_file, row_factory=sf.list_row )
        except Exception as e:
            gf.print_trace(e)
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue
       
        #TODO implement source specific treatment (process by source?) OR keep source information
        if not dt_30min:
            # in DWD data we need to replace the duration for 9z Tmin/Tmax obs
            sql = "UPDATE OR IGNORE obs SET duration='15h' WHERE element IN('TMAX_2m_syn','TMIN_2m_syn','TMIN_5cm_syn') AND strftime('%H', datetime) = '09' AND dataset IN('test','DWD','dwd_germany')"
            sql = "UPDATE OR IGNORE obs SET duration='1s' WHERE element LIKE 'CB%_2m_syn'"
            try:    db_loc.exe(sql)
            except: continue
            else:   db_loc.commit()
        
        """
        sql1=f"SELECT dataset,datetime,duration,element,value FROM obs WHERE element = '%s'{dt_30min}"
        sql2="INSERT INTO obs (dataset,datetime,duration,element,value) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING"
        
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
         
        sql = ("SELECT dataset,datetime,element,round(value) from obs WHERE element IN "
            "('CDC{i}_2m_syn', 'CB{i}_2m_syn'){dt_30min} ORDER BY datetime asc, element desc")
        
        # https://discourse.techart.online/t/python-group-nested-list-by-first-element/3637

        sql_values = set()

        for i in range(1,5):
            #print(sql.format(i=i))
            db_loc.exe(sql.format(i=i, dt_30min=dt_30min))
            data = db_loc.fetch()
            
            CL              = defaultdict(str)
            cloud_covers    = set()

            for j in data:
                if len(CL[j[0]]) == 0 and j[1] == f"CDC{i}_2m_syn" and j[0] not in cloud_covers:
                    CL[j[0]]    += str(int(j[-1]))
                    cloud_covers.add(j[0])
                elif len(CL[j[0]]) == 1 and j[1] == f"CB{i}_2m_syn" and j[0] in cloud_covers:
                    CB_in_30m   = int(j[-1]/30)
                    CL[j[0]]    += str(CB_in_30m).rjust(3,"0")

            CL = dict(CL)

            for k in CL:
                if len(CL[k]) == 1:
                    CL[k] += "///"
                sql_values.add( (k, f"CL{i}_2m_syn", CL[k]) )

        # duration is always 1s for cloud observations
        sql = "INSERT INTO obs (dataset,datetime,element,value,duration) VALUES(?,?,?,?,'1s') ON CONFLICT DO UPDATE SET value=excluded.value" #NOTHING"
        try:    db_loc.exemany(sql, sql_values)
        except: continue

        # https://stackoverflow.com/a/49975954
        sql = "DELETE FROM obs WHERE length(value) > 4 AND element LIKE 'CL%_2m_syn'"
        try:    db_loc.exe(sql)
        except: continue
       
        # derive cloud bases [CB?_2m_syn] and cloud covers in the 4 levels [CDC?_2m_syn]
        # from cloud levels [CL?_2m_syn] (usually provided by metwatch CSVs)
        sql = f"SELECT element,value FROM obs WHERE element REGEXP '(CB%_2m_syn|CDC%_2m_syn)'{dt_30min} ORDER BY datetime asc, element desc" 
        db_loc.row_factory = sf.dict_row
        db_loc.exe(sql)
         
        # reset row factory
        db_loc.row_factory = sf.default_row

        data = db_loc.fetch()


        # try to calculate QFF+QNH if no reduced pressure is present in obs and we have barometer height instead
        db = dc( config=cf.database, ro=1 )
        
        # we should actually prefer the barometer elevation over general elevation because they can differ a lot
        try:
            baro_height = db.get_station_baro_elev(loc)
        except sqlite3.OperationalError:
            baro_height = None
        
        #TODO if baro_elev is implemented in amalthea/main incomment this line and delete try/except above
        #baro_height = db.get_station_baro_elev(loc)

        if baro_height is None:
            baro_height     = db.get_station_elevation(loc)
            station_height  = copy(baro_height)
        else:
            station_height  = copy(baro_height)

        db.close()
        
        # do this for all metwatch elements which can be linked to a TR
        # derive [element, TR] from element and TR
        # derive [PRATE_1m_syn, TR] from PRATETR_1m_syn and TR
        
        # get all datetime where both elements are present and have a NOT NULL value
        
        sql = (f"SELECT DISTINCT datetime FROM obs WHERE element LIKE '%TR_%' AND "
            f"value IS NOT NULL{dt_30min} INTERSECT SELECT DISTINCT datetime FROM obs WHERE "
            f"element = 'TR' AND value IS NOT NULL{dt_30min}")
        db_loc.row_factory = sf.tuple_row
        db_loc.exe(sql)
        # reset row factory
        db_loc.row_factory = sf.default_row

        #sql_insert  = "INSERT INTO obs (dataset,datetime,element,value,duration) VALUES(?,?,'PRATE_1m_syn',?,?) ON CONFLICT DO NOTHING"
        sql_insert  = "INSERT INTO obs (dataset,datetime,element,value,duration) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING"
        prate_vals  = set()
        
        db_loc.row_factory = sf.dict_row

        for datetime in db_loc.fetch():
            datetime = datetime[0] 
            sql = (f"SELECT dataset,element,value FROM obs WHERE datetime = '{datetime}' AND element "
                f"LIKE '%TR%' ORDER BY element")
            db_loc.exe(sql)
            
            duration    = None
            value_TR    = None
            prev_elem   = ""

            for values in db_loc.fetch():
                
                dataset, element, value = values
                
                if element == "TR": duration    = str(value) + "h"
                else:               value_obs   = copy(value)
                
                # if we had element="TR" and a TR-dependent obs alternating plus value and duration
                if element != prev_elem and element != "TR" and value_obs and duration:
                    # add the observation to the sql values set
                    prate_vals.add( (dataset, datetime, element, value_obs, duration) )
        
        # reset row factory
        db_loc.row_factory = sf.default_row
        db_loc.exemany(sql_insert, prate_vals)


        # derive reduced pressure (QFF or QNH?) if only station pressure was reported
         
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
            sql_insert  = "INSERT INTO obs (dataset,datetime,element,value,duration) VALUES(?,?,?,?,'1s') ON CONFLICT DO NOTHING"
            prmsl_vals  = set()

            # try calculate PRMSL for all datetimes where only PRES is available
            sql = (f"SELECT dataset,datetime,value FROM obs WHERE element = 'PRES_0m_syn' AND value "
                f"IS NOT NULL{dt_30min}")
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
                    
                    # if dewpoint or relative humidity are present: use DWD reduction method
                    #https://www.dwd.de/DE/leistungen/pbfb_verlag_vub/pdf_einzelbaende/vub_2_binaer_barrierefrei.pdf?__blob=publicationFile&v=4 [page 106]
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
            
            sql_insert  = ("INSERT INTO obs (dataset,datetime,element,value,duration) VALUES"
                "(?,?,'DPT_2m_syn',?,'1s') ON CONFLICT DO NOTHING")
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
                        dpt_vals.add( (dataset, datetime, value_DPT) )
                
            db_loc.exemany(sql_insert, dpt_vals)
            

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
            
             
        db_loc.close(commit=True)
        
    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Derive obs elements from other parameters"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","d","t","P","A")
    cf          = cc(script_name, pos=["source"], flags=flags, info=info, verbose=False)
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
    replacements    = cf.script["replacements"]
    combinations    = cf.script["combinations"]

    if cf.script["aggregated"]:
        dt_30min = " AND strftime('%M', datetime) IN ('00','30')"
    else: dt_30min = ""

    #obs             = oc( cf, source, stage="forge" )
    db              = dc( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)
    
    # get SYNOP codes conversion dictionary to decode SYNOP codes
    #synop_codes     = gf.read_yaml("codes/synop")
    # get METAR codes conversion dictionary to decode METAR codes
    #metar_codes     = gf.read_yaml("codes/metar")
    
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
