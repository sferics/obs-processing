#!/usr/bin/env python
import global_functions as gf
import global_variables as gv
import sql_factories as sf
from database import database
from obs import obs

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

#TODO derive CLCMCH_syn (cloud amounts in the layers)
# CLCMCH_syn = TCC_LC_syn + TCC_MC_syn + TCC_HC_syn
# OR if no TCC_LC_sy, TCC_MC_syn, TCC_HC_syn:
# CLCMCH_syn = TCC_1C_syn + TCC_2C_syn + TCC_3C_syn
#TODO what about TCC_4C_syn ???

#TODO derive VIS_syn from MOR_syn, MOR_min, MOR_max, VIS_min, VIS_pre, VIS_run, VIS_sea
# if no VIS_syn: VIS_syn = MOR_syn
# [if no VIS_syn and no MOR_syn: VIS_min, MOR_min, VIS_pre, VIS_run, VIS_sea, MOR_max (priorities)]

# if key is not found, try to take replacements[key], else ignore

def derive_obs(stations):
    for loc in stations:

        sql_values = set()

        try: db_loc = database( f"/home/juri/data/stations_test/forge/{loc[0]}/{loc}.db", row_factory=sf.list_row, verbose=True, traceback=True )
        except Exception as e:
            if verbose:     print( f"Could not connect to database of station '{loc}'" )
            if traceback:   gf.print_trace(e)
            continue

        try: db_loc.exe( f"SELECT DISTINCT strftime('%Y', datetime) FROM obs" )
        except: continue

        #sql = "SELECT datetime,duration,element,value FROM obs WHERE element LIKE 'CL%_syn' GROUP BY datetime,duration,element"

        #sql = "SELECT datetime,duration,element,value FROM obs WHERE element LIKE 'TCC_%C_syn' GROUP BY datetime,duration,element"
        
        sql = "SELECT datetime,duration,element,value FROM obs WHERE element = %s"
        
        for replace in replacements:
            #db_loc.cur.con.row_factory = db_loc.list_factory
            db_loc.exe(sql % replacements[replace])
            data = db_loc.fetch()
            for i in data:
                i[2] = replace
                sql = "INSERT INTO obs VALUES(?,?,?,?) ON CONFLICT IGNORE"
                db_loc.exe(sql, i)

        for combine in combinations:
            pass

if __name__ == "__main__":
    
   #TODO implement source option! for now, just stick with test
    src = "test"

    config          = gf.read_yaml( "config.yaml" )
    db_settings     = config["database"]["settings"]
    config_script   = config["scripts"][sys.argv[0]]
    verbose         = config_script["verbose"]
    traceback       = config_script["traceback"]

    db              = database( config["database"] )

    clusters        = set(config_source["clusters"].split(","))
    stations        = db.get_stations( cluster ); db.close(commit=False)

    replacements = config_script["replacements"]
    combinations = config_script["combinations"]

    if config_script["multiprocessing"]:
        # number of processes
        npcs = config_script["multiprocessing"]
        import multiprocessing as mp
        from random import shuffle
        import numpy as np

        stations = list(stations)
        shuffle(stations)
        #stations_groups = gf.chunks(stations, npcs)
        station_groups = np.array_split(stations, npcs)

        for station_group in station_groups:
            p = mp.Process(target=derive_stations, args=(station_group,))
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

# take 5m wind as 10m wind if 10m wind not present

# derive wind direction from U and V

# derive total sunshine duration in min from h
# derive total sunshine duration in min from % (using astral package; see wetterturnier)

# derive precipitation amount from duration and intensity


if __name__ == "__main__":
    pass
