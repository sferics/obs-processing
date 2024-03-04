#!/usr/bin/env python
import os
from collections import defaultdict
import global_functions as gf
import global_variables as gv
import sql_factories as sf
from database import DatabaseClass
from config import ConfigClass

#TODO HIGH PRIORITY! ESSENTIAL!

  #ClNCmNChN: [CLCMCH_syn,  ~, 1, 0]  # Wolkenarten in den Stockwerken                   (zB: Cu2Ac3Cs6) -> 236
  #ClNCmNChN: [CLCMCH_syn,  ~, 1, 0]  # -> Wolkenmenegen in den Stockwerken              (zB: 3451, 1///)
  #NC1XXX:    [CL1_syn,     ~, 1, 0]  # unterste Wolkenschicht Bedeckung/Art/Untergrenze (zB: 2ST020) -> 2020
  #NC2XXX:    [CL2_syn,     ~, 1, 0]  # 2.Wolkenschicht                                  (zB: 4AC100) -> 4100
  #NC3XXX:    [CL3_syn,     ~, 1, 0]  # 3.Wolkenschicht                                  (zB: 5CS300) -> 5300
  #NC4XXX:    [CL4_syn,     ~, 1, 0]  # 4.Wolkenschicht                                  (zB: 2CB080) -> 2080
  #NC1XXX:    [CL?_syn,     ~, 1, 0]  # Wolkenschicht Bedeckung+Untergrenze              (zB: 1015, 5300, 2080)

#TODO if no TCC_LC_syn: take TCC_ceiling_syn

#TODO derive total cloud cover per height level (CL, CM, CH)

#TODO derive cloud layers (CL_?) from cloud bases and cloud amounts

# only in station_test: first devide cloud height by 30

# CL?_syn = TCC_?C_syn + CB?_syn
# if no TCC_[1-3]C_syn present:
# CL1_syn = TCC_LC_syn + CB1_syn
# CL2_syn = TCC_MC_syn + CB2_syn
# CL3_syn = TCC_HC_syn + CB3_syn

#TODO derive VIS_syn from MOR_syn, MOR_min, MOR_max, VIS_min, VIS_pre, VIS_run, VIS_sea
# if no VIS_syn: VIS_syn = MOR_syn
# [if no VIS_syn and no MOR_syn: VIS_min, MOR_min, VIS_pre, VIS_run, VIS_sea, MOR_max (priorities)]

# if key is not found, try to take replacements[key], else ignore

def derive_obs(stations):
    
    for loc in stations:

        sql_values = set()
        
        db_file = f"{output}/forge/{loc[0]}/{loc}.db"
        try: db_loc = DatabaseClass( db_file, row_factory=sf.list_row )
        except Exception as e:
            gf.print_trace(e)
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue
        
        if source in {"test", "DWD", "dwd_germany"}:
            # in DWD data we need to replace the duration for 9z Tmin/Tmax obs
            sql = "UPDATE OR IGNORE obs SET duration='15h' WHERE element IN('Tmax_2m_syn','Tmax_5cm_syn','Tmin_2m_syn','Tmin_5cm_syn') AND strftime('%H', datetime) = '09'"
            #sql = "UPDATE OR IGNORE obs SET duration='1s' WHERE element LIKE 'CB%_syn'"
            #sql = "UPDATE OR IGNORE obs SET element='TCC_1C_syn' WHERE element='TCC_ceiling_syn'"
            try:    db_loc.exe(sql)
            except: continue
            else:   db_loc.commit()

        """
        sql1="SELECT datetime,duration,element,value FROM obs WHERE element = '%s'"
        sql2="INSERT INTO obs (datetime,duration,element,value) VALUES(?,?,?,?) ON CONFLICT IGNORE"

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
    
        sql = "SELECT datetime,element,round(value) from obs WHERE element IN ('TCC_{i}C_syn', 'CB{i}_syn') ORDER BY datetime asc, element desc"
        
        # https://discourse.techart.online/t/python-group-nested-list-by-first-element/3637

        sql_values = set()

        for i in range(1,5):
            #print(sql.format(i=i))
            db_loc.exe(sql.format(i=i))
            data = db_loc.fetch()
            
            CL              = defaultdict(str)
            cloud_covers    = set()

            for j in data:
                if len(CL[j[0]]) == 0 and j[1] == f"TCC_{i}C_syn" and j[0] not in cloud_covers:
                    CL[j[0]]    += str(int(j[-1]))
                    cloud_covers.add(j[0])
                elif len(CL[j[0]]) == 1 and j[1] == f"CB{i}_syn" and j[0] in cloud_covers:
                    CL[j[0]]    += str(int(j[-1])).rjust(3,"0")

            CL = dict(CL)

            for k in CL:
                if len(CL[k]) == 1:
                    CL[k] += "///"
                sql_values.add( (k, f"CL{i}_syn", CL[k]) )
        
        # duration is always 1s for cloud observations
        sql = "INSERT INTO obs (datetime,element,value,duration) VALUES(?,?,?,'1s') ON CONFLICT DO UPDATE SET value=excluded.value" #NOTHING"
        try:    db_loc.exemany(sql, sql_values)
        except: continue

        # https://stackoverflow.com/a/49975954
        sql = "DELETE FROM obs WHERE length(value) > 4 AND element LIKE 'CL%_syn'"
        try:    db_loc.exe(sql)
        except: continue
        
        db_loc.close(commit=True)

    return


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Derive obs elements from other parameters"
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
    replacements    = cf.script["replacements"]
    combinations    = cf.script["combinations"]

    obs             = ObsClass( cf, source, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
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
            p = mp.Process(target=derive_obs, args=(station_group,))
            p.start()

    else: derive_obs(stations)

    #TODO medium priority, nice-to-have...

    #TODO try to calculate QFF and QNH if no reduced pressure is present in obs and we have barometer height instead
    """
    if meta["heightOfBarometerAboveMeanSeaLevel"] not in null_vals:
        baro_height = meta["heightOfBarometerAboveMeanSeaLevel"]
    elif meta["heightOfStationGroundAboveMeanSeaLevel"] not in null_vals:
        baro_height = meta["heightOfStationGroundAboveMeanSeaLevel"]
    elif meta["heightOfStation"] not in null_vals:
        baro_height = meta["heightOfStation"]
    elif meta["elevation"] not in null_vals:
        baro_height = meta["elevation"]
    else: baro_height = None
    """

    #TODO derive reduced pressure (QFF or QNH?) if only station pressure was reported
    """
    MSL = "pressureReducedToMeanSeaLevel"
    if MSL not in obs and baro_height is not None and ("pressure" in obs or "nonCoordinatePressure" in obs):
        try: ppp = obs["pressure"][0]
        except: ppp = obs["conCoordinatePressure"][0]

        obs_tp["MSL_ms_syn"] = gf.qff( PPP, baro_height )
        obs_tp["MSL_ms_met"] = gf.qnh( PPP, baro_height )
    """


    #TODO LOW priority, not really needed at the moment

    #TODO calculate derivation of dewpoint temperature here, add unit conversions...
    """
    dp = "dewpointTemperature"; dp2 = "dewpointTemperature2m"
    T = "airTemperature"; T2 = "airTemperatureAt2m"; rh = "relativeHumidity"

    # if we already has the dewpoint temperature at 2m height, skip!
    if dp2 in obs or (dp in obs and sensor_height[0] == 2):
        pass
    elif rh in obs and ( (T in obs and sensor_height[0] == 2) or T2 in obs ): 
        if T in obs: T = obs[T][0]
        else: T = obs[T2][0]
        rh = obs[rh][0]
        
        obs[dp2] = ( gf.rh2dp( rh, T ), "2s" )
    """

    #TODO take 5m wind as 10m wind if 10m wind not present

    #TODO derive wind direction from U and V

    #TODO derive total sunshine duration in min from h
    #TODO derive total sunshine duration in min from % (using astral package; see wetterturnier)

    #TODO derive precipitation amount from duration and intensity
