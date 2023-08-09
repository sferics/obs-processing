import re
from copy import copy
import global_functions as gf
from datetime import datetime as dt, timedelta as td


to_datetime = lambda meta : dt(meta["year"], meta["month"], meta["day"], meta["hour"], meta["minute"])

clear   = lambda keyname : str( re.sub( r"#[0-9]+#", '', keyname ) )
number  = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key  = lambda key,num : "#{num}#{key}"


def translate_key( bufr_translation, key, value, duration, h=None ):

    key_db = bufr_translation[key]

    if key_db is None: return None
    if h is not None: # we are looking for a specific height/depth key, key_db is a dict now
        try:    key_db = key_db[h]
        except  KeyError:
            print("height ERROR")
            print(key, value, h)
            return None, None, None
    else: bufr_translation[key]

    # add units + scale conversion
    value = float(value) * key_db[2] + key_db[3]

    # if we already got a timePeriod from BUFR we dont need to translate it with the yaml dict
    if not duration: duration = key_db[1]

    return key_db[0], value, duration


def translate_key_units( bufr_translation, key, value, duration, h=None, unit=None ):

    key_db = bufr_translation[key]

    if key_db is None: return None
    if h is not None: # we are looking for a specific height/depth key, key_db is a dict now
        try:    key_db = key_db[h]
        except  KeyError:
            print("height ERROR")
            print(key, value, h)
            return None, None, None
    elif unit is not None:
        # if key_db is now a list of dicts (units), we want to merge them together
        key_db = gf.merge_list_of_dicts( key_db )
        try:    key_db = key_db[unit]
        except  KeyError:
            print("unit   ERROR")
            return None, None, None
    else: bufr_translation[key]

    # add units + scale conversion
    value = float(value) * key_db[2] + key_db[3]

    # if we already got a timePeriod from BUFR we dont need to translate it with the yaml dict
    if not duration: duration = key_db[1]

    return key_db[0], value, duration


def convert_keys( obs,dataset,modifier_keys,height_depth_keys,bufr_translation,bufr_flags ):

    time_periods = bufr_translation["timePeriod"]

    #if verbose: print(obs)
    obs_db = {}
    #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

    for file in obs:
        for location in obs[file]:

            if location not in obs_db: obs_db[location] = set()

            duration        = ""
            vertical_sigf   = 0
            cloud_cover     = None
            cloud_ceiling   = float("inf")
            cloud_amounts, cloud_bases  = set(), set()
            sensor_height, sensor_depth = None, None

            try:    datetime = obs[file][location][0]; skip_next = 1
            except: continue

            # delete the last element of the list if it's a modifier key; it can go modify the weather of Island
            try:
                if len(obs[file][location]) > 1 and obs[file][location][-1][0] in modifier_keys:
                    del obs[file][location][-1]
            except: pass

            ix_data = len(obs[file][location])-1

            for ix, data in enumerate(obs[file][location]):

                if skip_next: skip_next -= 1; continue

                # if datetime present which indicates new obs or end of iteration
                if type(data) == dt or ix == ix_data:
                    if type(data) == dt: datetime_db = data; duration = ""

                    if cloud_ceiling < float("inf"):
                        key = "heightOfBaseOfCloud"
                        element, val_db, duration = translate_key(bufr_translation, key, cloud_ceiling, duration )
                        obs_db[location].add( (datetime_db, dataset, file, element, val_db, duration) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = translate_key(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (datetime_db, dataset, file, element, val_db, duration) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = translate_key(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (datetime_db, dataset, file, element, val_db, duration) )
                        cloud_bases = set()

                    duration        = ""
                    vertical_sigf   = 0
                    cloud_cover     = None
                    cloud_ceiling   = float("inf")
                    cloud_amounts, cloud_bases  = set(), set()
                    sensor_height, sensor_depth = None, None
                    continue

                key, val_obs = data[0], data[1]

                if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                else:                           datetime_db = copy(datetime)

                if key == "timePeriod":
                    duration    = time_periods[val_obs]
                    datetime_db = copy(datetime); continue
                elif key == "verticalSignificanceSurfaceObservations":
                    vertical_sigf = bufr_flags[key][val_obs]; continue
                elif key == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform":
                    sensor_height = float(val_obs); continue
                elif key == "depthBelowLandSurface":
                    sensor_depth  = float(val_obs) * (-1); continue
                else:
                    if key in height_depth_keys:
                        if key == "soilTemperature":
                            h = copy(sensor_depth)
                            if not h or h > 0: h = -0.05
                        else:
                            h = copy(sensor_height)
                            if not h or h > 1: h = 2.0

                        element, val_db, duration = translate_key(bufr_translation, key, val_obs, duration, h=h)

                    elif key == "heightOfBaseOfCloud":
                        # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                        if val_obs < cloud_ceiling: cloud_ceiling = copy(val_obs)

                        # also we want to save all unique cloud levels (base heights) which where observed
                        cloud_bases.add(val_obs); continue

                    elif key == "cloudCoverTotal":
                        cloud_cover = copy(val_obs)
                        element, val_db, duration = translate_key(bufr_translation, key, cloud_cover, duration)
                        if dataset in {"DWD","test"}: val_db = int(val_db)

                    elif key == "cloudAmount":
                        if cloud_cover is None: cloud_amounts.add( val_obs )

                        element, val_db, duration = translate_key(bufr_translation, key, val_obs, duration, h=vertical_sigf)
                        if not vertical_sigf: continue

                    else: element, val_db, duration = translate_key(bufr_translation, key, val_obs, duration)
                    if element is not None:
                        obs_db[location].add( ( datetime_db, dataset, file, element, val_db, duration ) )
                    else: print(f"element is None for key: {key}, value: {val_obs}")

    return obs_db


def convert_keys_units(obs, dataset, modifier_keys, unit_keys, height_depth_keys, bufr_translation, bufr_flags):

    #if verbose: print(obs)
    obs_db = {}
    #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

    for file in obs:

        for location in obs[file]:

            if location not in obs_db: obs_db[location] = set()

            for datetime in obs[file][location]:

                if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                else:                           datetime_db = copy(datetime)

                duration        = ""
                vertical_sigf   = 0
                cloud_cover     = None
                cloud_ceiling   = float("inf")
                cloud_amounts, cloud_bases  = set(), set()
                sensor_height, sensor_depth = None, None

                # delete the last element of the list if it's a modifier key; it can go modify the weather of Island
                if len(obs[file][location][datetime]) > 1 and obs[file][location][datetime][-1][0] in modifier_keys:
                    del obs[file][location][datetime][-1]

                for data in obs[file][location][datetime]:

                    key, val_obs = data[0], data[1]

                    if key == "timePeriod":
                        duration    = copy(val_obs)
                        datetime_db = copy(datetime)
                    elif key == "verticalSignificanceSurfaceObservations": vertical_sigf = bufr_flags[key][val_obs]
                    elif key == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform":
                        sensor_height = float(val_obs)
                    elif key == "depthBelowLandSurface":
                        sensor_depth  = float(val_obs) * (-1)
                    else:
                        if key in height_depth_keys:
                            if key == "soilTemperature":
                                h = copy(sensor_depth)
                                if not h or h > 0: h = -0.05
                            else:
                                h = copy(sensor_height)
                                if not h or h > 1: h = 2.0

                            element, val_db, duration = translate_key_units(bufr_translation, key, val_obs, duration, h=h)

                        elif key in unit_keys:
                            if key == "heightOfBaseOfCloud":
                                # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                if val_obs < cloud_ceiling:
                                    cloud_ceiling   = copy(val_obs)
                                    unit_ceiling    = data[2]

                                # also we want to save all unique cloud levels (base heights) which where observed
                                cloud_bases.add(val_obs)
                                continue

                            else: element,val_db,duration = translate_key_units(bufr_translation, key, val_obs, duration, unit=data[2])

                        elif key == "cloudCoverTotal":
                            cloud_cover = copy(val_obs)
                            element, val_db, duration = translate_key_units(bufr_translation, key, cloud_cover, duration)
                            if dataset in {"DWD","test"}: val_db = int(val_db)

                        elif key == "cloudAmount":
                            if cloud_cover is None: cloud_amounts.add( val_obs )

                            element, val_db, duration = translate_key_units(bufr_translation, key, val_obs, duration, h=vertical_sigf )
                            if not vertical_sigf: continue

                        else: element, val_db, duration = translate_key_units(bufr_translation, key, val_obs, duration)
                        if element is not None:
                            obs_db[location].add( ( datetime_db, dataset, file, element, val_db, duration ) )
                        else: print(f"element is None for key: {key}, value: {val_obs}")

                if cloud_ceiling < float("inf"):
                    key = "heightOfBaseOfCloud"
                    element, val_db, duration = translate_key_units(bufr_translation, key, cloud_ceiling, duration, unit=unit_ceiling )
                    obs_db[location].add( (datetime_db, dataset, file, element, val_db, duration) )

                if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                    element, val_db, duration = translate_key_units(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                    obs_db[location].add( (datetime_db, dataset, file, element, val_db, duration) )

                if cloud_bases:
                    cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                    for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                        element, val_db, duration = translate_key_units(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                        obs_db[location].add( (datetime_db, dataset, file, element, val_db, duration) )

    return obs_db
