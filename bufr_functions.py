import re, sys
from copy import copy
import global_functions as gf
from datetime import datetime as dt, timedelta as td, timezone as tz


to_datetime = lambda meta : dt(meta["year"],meta["month"],meta["day"],meta["hour"],meta["minute"])
clear       = lambda keyname : str( re.sub( r"#[0-9]+#", '', keyname ) )
number      = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
to_key      = lambda key,num : f"#{num}#{key}"


def translate_key( bufr_translation, key, value, duration, h=None ):

    key_db = bufr_translation[key]

    if key_db is None: return None, None, None
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
    if duration is None: duration = "NULL"

    return key_db[0], value, duration


def translate_key_units( bufr_translation, key, value, duration, h=None, unit=None ):

    key_db = bufr_translation[key]

    if key_db is None: return None, None, None
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
    if duration is None: duration = "NULL"

    return key_db[0], value, duration


def convert_keys_se_flat( obs,dataset,modifier_keys,height_depth_keys,bufr_translation,bufr_flags,verbose=False ):

    time_periods = bufr_translation["timePeriod"]

    if verbose: print(obs)
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

            try:    datetime_db = obs[file][location][0]; skip_next = 1
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
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = translate_key(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = translate_key(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
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
                
                #datetime_db = copy(datetime)

                if key == "timePeriod":
                    duration    = time_periods[val_obs]#; continue
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
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                    else: print(f"element is None for key: {key}, value: {val_obs}")

    return obs_db


def convert_keys_se(obs, dataset, modifier_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=False):

    time_periods = bufr_translation["timePeriod"]

    if verbose: print(obs)
    obs_db = {}
    #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

    counter={}

    for file in obs:

        for location in obs[file]:

            if location not in obs_db: obs_db[location] = set()

            for datetime in obs[file][location]:

                if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                else:                           datetime_db = copy(datetime)

                duration        = ""
                offset          = 0
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
                        try:    counter[key] += 1
                        except: counter[key] = 0
                        duration    = time_periods[val_obs]
                        """
                        if val_obs == -10:
                            offset += 10
                            datetime_db -= td(minutes=offset)
                        else:               datetime_db = copy(datetime)
                        """
                        datetime_db = copy(datetime)
                    elif key == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform":
                        try:    counter[key] += 1
                        except: counter[key] = 0
                        sensor_height = float(val_obs)
                    elif key == "verticalSignificanceSurfaceObservations":
                        try:    counter[key] += 1
                        except: counter[key] = 0
                        vertical_sigf = bufr_flags[key][val_obs]
                    elif key == "depthBelowLandSurface":
                        try:    counter[key] += 1
                        except: counter[key] = 0
                        sensor_depth  = float(val_obs) * (-1)
                    else:
                        if key in height_depth_keys:
                            if key == "soilTemperature":
                                h = copy(sensor_depth)
                                if not h or h >= 0: h = -0.05
                            else:
                                h = copy(sensor_height)
                                if not h or h >= 1: h = 2.0

                            element, val_db, duration = translate_key(bufr_translation, key, val_obs, duration, h=h)

                        elif key == "heightOfBaseOfCloud":
                                # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                if val_obs < cloud_ceiling:
                                    cloud_ceiling   = copy(val_obs)

                                # also we want to save all unique cloud levels (base heights) which where observed
                                cloud_bases.add(val_obs)
                                continue

                        elif key == "cloudCoverTotal":
                            cloud_cover = copy(val_obs)
                            element, val_db, duration = translate_key(bufr_translation, key, cloud_cover, duration)
                            if dataset in {"DWD","test"}: val_db = int(val_db)

                        elif key == "cloudAmount":
                            if cloud_cover is None: cloud_amounts.add( val_obs )

                            element, val_db, duration = translate_key(bufr_translation, key, val_obs, duration, h=vertical_sigf )
                            if not vertical_sigf: continue

                        else: element, val_db, duration = translate_key(bufr_translation, key, val_obs, duration)
                        if element is not None:
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                        else: print(f"element is None for key: {key}, value: {val_obs}")

                if cloud_ceiling < float("inf"):
                    key = "heightOfBaseOfCloud"
                    element, val_db, duration = translate_key(bufr_translation, key, cloud_ceiling, duration )
                    obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                    element, val_db, duration = translate_key(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                    obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                if cloud_bases:
                    cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                    for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                        element, val_db, duration = translate_key(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

    #if verbose: print(obs_db)
    print(counter)
    return obs_db


def convert_keys_pd( obs,dataset,modifier_keys,height_depth_keys,bufr_translation,bufr_flags,fixed_duration,verbose=False ):
    
    time_periods = bufr_translation["timePeriod"]

    if verbose: print(obs)
    obs_db = {}
    #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

    for file in obs:
        for location in obs[file]:
            if location not in obs_db: obs_db[location] = set()
            for datetime in obs[file][location]:
                #datetime_obs   = datetime.to_pydatetime()
                datetime_db     = datetime.to_pydatetime()
                #if datetime_obs.minute in {0,30}:  datetime_db = datetime - td(minutes=10)
                #else:                              datetime_db = copy(datetime)

                for time_period in obs[file][location][datetime]:
                    
                    if time_period: duration = time_periods[time_period]
                    else:           duration = ""
                    vertical_sigf   = 0
                    cloud_cover     = None
                    cloud_ceiling   = float("inf")
                    cloud_amounts, cloud_bases  = set(), set()
                    sensor_height, sensor_depth = None, None

                    for data in obs[file][location][datetime][time_period]:
                        
                        key, val_obs = data[0], data[1]
                            
                        #if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                        #else:                           datetime_db = copy(datetime)

                        if key == "verticalSignificanceSurfaceObservations":
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


    return obs_db


def convert_keys_00(obs, dataset, modifier_keys, unit_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=False):

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
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                        else: print(f"element is None for key: {key}, value: {val_obs}")

                if cloud_ceiling < float("inf"):
                    key = "heightOfBaseOfCloud"
                    element, val_db, duration = translate_key_units(bufr_translation, key, cloud_ceiling, duration, unit=unit_ceiling )
                    obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                    element, val_db, duration = translate_key_units(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                    obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                if cloud_bases:
                    cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                    for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                        element, val_db, duration = translate_key_units(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

    return obs_db


def convert_keys_us(obs, dataset, modifier_keys, unit_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=False):

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
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                        else: print(f"element is None for key: {key}, value: {val_obs}")

                if cloud_ceiling < float("inf"):
                    key = "heightOfBaseOfCloud"
                    element, val_db, duration = translate_key_units(bufr_translation, key, cloud_ceiling, duration, unit=unit_ceiling )
                    obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                    element, val_db, duration = translate_key_units(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                    obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                if cloud_bases:
                    cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                    for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                        element, val_db, duration = translate_key_units(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

    return obs_db


def decode_bufr_se_flat( source=None, file=None, known_stations=None, pid_file=None ):
    if source:
        config_source   = config_sources[source]
        if "bufr" in config_source: config_bufr = config_source["bufr"]
        else: return
        bufr_dir        = config_bufr["dir"] + "/"
        ext             = config_bufr["ext"]
        try:    clusters = set(config_source["clusters"].split(","))
        except: clusters = None
        db = database_class(db_file, timeout=timeout_db, traceback=traceback)
        for i in range(max_retries):
            try:    known_stations = db.get_stations( clusters )
            except: time.sleep(timeout_station)
            else:   break
        if i == max_retries - 1: sys.exit(f"Can't access main database, tried {max_retries} times. Is it locked?")
        if "glob" in config_bufr and config_bufr["glob"]:   ext = f"{config_bufr['glob']}.{ext}"
        else:                                               ext = f"*.{ext}" #TODO add multiple extensions (list)
        files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + ext )))
        if args.redo:   skip_files  = set()
        else:           skip_files  = set(db.get_files_with_status( config_script["skip_status"], source ))
        files_to_parse = list( files_in_dir - skip_files )
        if config_script["sort_files"]: files_to_parse = sorted(files_to_parse)
        if config_script["max_files"]:  files_to_parse = files_to_parse[:config_script["max_files"]]
        if verbose: print("#FILES in DIR:  ",len(files_in_dir)); print("#FILES to skip: ",len(skip_files)); print("#FILES to parse:",len(files_to_parse))
        gf.create_dir( bufr_dir )
        file_IDs = {}
        for FILE in files_to_parse:
            file_path = gf.get_file_path( bufr_dir + FILE )
            file_date = gf.get_file_date( file_path )
            if args.redo:
                ID = db.get_file_id(FILE, file_path)
                if not ID: ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)
                file_IDs[FILE] = ID
            else: file_IDs[FILE] = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)
        db.close(commit=True)
    elif file:
        FILE            = file.split("/")[-1]; files_to_parse  = (FILE,)
        file_path       = gf.get_file_path(args.file); file_date       = gf.get_file_date(args.file)
        bufr_dir        = "/".join(file.split("/")[:-1]) + "/"
        source          = args.extra # default: extra
        db = database_class(db_file, timeout=timeout_db, traceback=traceback)
        known_stations  = db.get_stations()
        ID = db.get_file_id(FILE, file_path)
        if ID:  db.set_file_status(ID,"locked")
        else:   ID = db.register_file(FILE,file_path,source,status="locked",date=file_date,verbose=verbose)
        db.close(commit=True)
        file_IDs = {FILE:ID}
    obs, file_statuses = {}, set()
    for FILE in files_to_parse:
        with open(bufr_dir + FILE, "rb") as f:
            try:
                ID = file_IDs[FILE]
                if not ID: continue
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    file_statuses.add( ("empty", ID) )
                    if verbose: print(f"EMPTY:  '{FILE}'")
                    continue
                ec.codes_set(bufr, "skipExtraKeyAttributes", 1); ec.codes_set(bufr, "unpack", 1)
            except Exception as e:
                log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                if verbose: print(log_str)
                if traceback: gf.print_trace(e)
                file_statuses.add( ("error", ID) )
                continue
            else: obs[ID] = {} #shelve.open(f"shelves/{ID}", writeback=True) #{}
            iterid = ec.codes_bufr_keys_iterator_new(bufr, namespace=None)
            for fun in (ec.codes_skip_duplicates, ec.codes_skip_computed, ec.codes_skip_function): fun(iterid)
            meta, typical = {}, {}; valid_obs = False; location = None; skip_next = 10; subset, new_obs = 0, 0; skip_obs = False; last_key = None
            while ec.codes_bufr_keys_iterator_next(iterid) is not None:
                if skip_next: skip_next -= 1; continue
                key = ec.codes_bufr_keys_iterator_get_name(iterid)
                if last_key == "typical" and last_key not in key:
                    last_key = None; skip_next = 3; continue
                if key == "subsetNumber":
                    if subset > 0: meta = {}; location = None; valid_obs = False; #skip_obs = False
                    subset += 1; continue
                elif skip_obs: continue
                clear_key = clear(key)
                if clear_key not in relevant_keys: continue
                if valid_obs:
                    if datetime not in obs[ID][location]:
                        obs[ID][location].append(datetime)
                    if clear_key in bufr_mod_keys:
                        try: value = ec.codes_get( bufr, key )
                        except Exception as e:
                            if verbose: print(FILE, key, e)
                            if traceback: gf.print_trace(e)
                            log_str = f"ERROR:  '{FILE}' ({e})"; log.error(log_str)
                            if verbose: print(log_str)
                            continue
                        if clear_key == "delayedDescriptorReplicationFactor":
                            if value == 10:
                                skip_next = 10
                                try:    del obs[ID][location][-1]
                                except: pass
                            continue
                        if value not in null_vals:
                            obs[ID][location].append(  (clear_key, value) )
                            if clear_key in modifier_keys:
                                try:
                                    if clear_key == obs[ID][location][-2][0]:
                                        del obs[ID][location][-2]
                                except: pass
                            else: new_obs += 1
                else:
                    if not subset and key in typical_keys:
                        typical[key]    = ec.codes_get( bufr, key )
                        if typical[key] in null_vals: del typical[key]
                        last_key        = "typical"
                        continue
                    if clear_key in station_keys:
                        meta[clear_key] = ec.codes_get(bufr, key)
                        if meta[clear_key] in null_vals: del meta[clear_key]; continue
                        if "shortStationName" in meta:
                            location = meta["shortStationName"]
                            station_type = "dwd"; skip_next = 4
                        elif { "stationNumber", "blockNumber" }.issubset( set(meta) ):
                            location = str(meta["stationNumber"] + meta["blockNumber"] * 1000).rjust(5,"0") + "0"
                            station_type = "wmo"
                            if source in {"test","DWD","COD","NOAA"}: skip_next = 2
                        if location:
                            if location not in known_stations:
                                meta = {}; location = None; skip_obs = True
                                if source in {"test","DWD","COD","KNMI","RMI","NOAA"}:
                                    if station_type == "wmo":   skip_next = 11
                                    elif station_type == "dwd": skip_next = 13
                            else: obs[ID][location] = []
                    elif location:
                        if clear_key in time_keys: # {year, month, day, hour, minute}
                            meta[clear_key] = ec.codes_get_long(bufr, key)
                            if meta[clear_key] in null_vals: del meta[clear_key]
                            if clear_key == "minute":
                                valid_obs = time_keys.issubset(meta)
                                if valid_obs:
                                    datetime = to_datetime(meta)
                                    if source in {"test","DWD","COD","NOAA"}: skip_next = 4
                                elif time_keys_hour.issubset(meta):
                                    meta["minute"] = 0; valid_obs = True
                                    datetime = to_datetime(meta)
                                elif typical:
                                    for i,j in zip(sorted_time_keys, sorted_typical_keys):
                                        try:    meta[i] = int(typical[j])
                                        except: pass
                                    if time_keys_hour.issubset(meta):
                                        meta["minute"] = 0; valid_obs = True; datetime = to_datetime(meta); continue
                                    if not {"year","month","day"}.issubset(set(meta)) and "typicalDate" in typical:
                                        typical_date    = typical["typicalDate"]
                                        meta["year"]    = int(typical_date[:4]); meta["month"]   = int(typical_date[4:6]); meta["day"]     = int(typical_date[-2:])
                                    else: skip_obs = True; continue
                                    if ("hour" not in meta or "minute" not in meta) and "typicalTime" in typical:
                                        typical_time    = typical["typicalTime"]
                                        if "hour" not in meta: meta["hour"] = int(typical_time[:2])
                                        if "minute" not in meta: meta["minute"] = int(typical_time[2:4])
                                    else: skip_obs = True; continue
                                else: skip_obs = True
            ec.codes_keys_iterator_delete(iterid)
        ec.codes_release(bufr)
        if new_obs: file_statuses.add( ("parsed", ID) );    log.debug(f"PARSED: '{FILE}'")
        else:       file_statuses.add( ("empty", ID) );     log.info(f"EMPTY:  '{FILE}'")
        memory_free = psutil.virtual_memory()[1] // 1024**2
        if memory_free <= config_script["min_ram"]:
            db = database_class(db_file, timeout=timeout_db, traceback=traceback)
            db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db)
            db.close()
            print("Too much RAM used, RESTARTING...")
            obs_db = convert_keys_se_flat(obs, source, modifier_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=verbose)
            if obs_db: obs.to_station_databases( obs_db, output_path, max_retries, timeout_station, verbose )
            if pid_file: os.remove( pid_file )
            exe = sys.executable # restart program with same arguments
            os.execl(exe, exe, * sys.argv); sys.exit()
    db = database_class(db_file, timeout=timeout_db, traceback=traceback)
    db.set_file_statuses(file_statuses, retries=max_retries, timeout=timeout_db); db.close()
    obs_db = convert_keys_se_flat(obs, source, modifier_keys, height_depth_keys, bufr_translation, bufr_flags, verbose=verbose)

