#!/usr/bin/env python
import global_functions as gf

#TODO derive cloud layers (CL_?) from cloud bases and cloud amounts

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

#TODO derive VIS_syn from MOR, MOR_min, MOR_max, VIS_min, VIS_pre, VIS_run

# try to calculate QFF and QNH if no reduced pressure is present in obs and we have barometer height instead
if meta["heightOfBarometerAboveMeanSeaLevel"] not in null_vals:
    baro_height = meta["heightOfBarometerAboveMeanSeaLevel"]
elif meta["heightOfStationGroundAboveMeanSeaLevel"] not in null_vals:
    baro_height = meta["heightOfStationGroundAboveMeanSeaLevel"]
elif meta["heightOfStation"] not in null_vals:
    baro_height = meta["heightOfStation"]
elif meta["elevation"] not in null_vals:
    baro_height = meta["elevation"]
else: baro_height = None


#TODO derive reduced pressure (QFF or QNH?) if only station pressure was reported
MSL = "pressureReducedToMeanSeaLevel"
if MSL not in obs and baro_height is not None and ("pressure" in obs or "nonCoordinatePressure" in obs):
    try: ppp = obs["pressure"][0]
    except: ppp = obs["conCoordinatePressure"][0]

    obs_tp["MSL_ms_syn"] = gf.qff( PPP, baro_height )
    obs_tp["MSL_ms_met"] = gf.qnh( PPP, baro_height )
