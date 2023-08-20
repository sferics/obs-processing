import re
import logging as log
import eccodes as ec
from copy import copy
from datetime import datetime as dt
import global_functions as gf
import global_variables as gv

class bufr:
    def __init__(self, config={"log_level":"ERROR", "verbose":0, "traceback":0, "debug":"0"}, script="us"):
        
        # parse all keys and values of config dict into namespace, a bit like in database.py
        for i in config:
            exec( f'self.{i} = "{config[i]}"' )
            try:    exec( f'self.{i} = int(self.{i})' )
            except: pass
            if self.verbose: print( i, "=", config[i] )

        # check for mandatory class attributes
        mandatory = ("log_level", "verbose", "traceback", "bufr_translation", "bufr_flags", "dev_mode", "output_path", "stations", "clusters") #,output_oper
        for attr in mandatory: assert( hasattr(self, attr) )

        assert(self.log_level in gv.log_levels)
        log.basicConfig(filename="bufr.log", filemode="w", level=eval(f"log.{self.log_level}"))

        if not hasattr(self, "max_files"): self.max_files=0

        # common key names for BUFR obs decoding scrips
        # timePeriod is very frequently used so we shorten it up a lot
        self.tp                 = "timePeriod"
        self.obs_sequence       = "observationSequenceNumber"
        self.replication        = "delayedDescriptorReplicationFactor"
        self.sensor_height      = "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform"
        self.sensor_depth       = "depthBelowLandSurface"
        self.vertical_signf     = "verticalSignificanceSurfaceObservations"
        self.modifier_keys      = frozenset({self.sensor_height, self.sensor_depth, self.vertical_signf, self.tp})
        
        self.station_keys       = frozenset({"shortStationName", "stationNumber", "blockNumber"})
        self.time_keys          = ("year", "month", "day", "hour", "minute")
        self.set_time_keys      = frozenset(self.time_keys)
        self.typical_time_keys  = ( "typical"+i.capitalize() for i in self.time_keys )
        self.typical_datetime   = frozenset({"typicalDate","typicalTime"})
        self.typical_keys       = frozenset( set(self.typical_time_keys) | self.typical_datetime )

        self.null_vals          = frozenset( {ec.CODES_MISSING_LONG, ec.CODES_MISSING_DOUBLE} ) # (2147483647, -1e+100)
        self.meta_ignore_vals   = frozenset( {"null", "NULL", "MISSING", "XXXX", " ", ""} )
        self.meta_null_vals     = frozenset( self.null_vals | self.meta_ignore_vals )
        
        self.skip_status        = frozenset( {"locked_%", "error", "empty", "parsed"} )

        # parse the BUFR translation and bufr flags yaml files into dictionaries
        self.bufr_translation   = gf.read_yaml(self.bufr_translation)
        self.bufr_flags         = gf.read_yaml(self.bufr_flags)

        # remove unit translations and pseudo-key "cloudType"
        self.bufr_translation_keys  = tuple(self.bufr_translation)[6:]
        
        # get special types of keys
        self.depth_keys, self.height_keys = set(), set()

        match script:
            
            case "us":
                
                self.unit_keys = set()
                
                for i in self.bufr_translation_keys:
                    if type(self.bufr_translation[i]) == list and type(self.bufr_translation[i][0]) == dict:
                        self.unit_keys.add(i)
                    elif type(self.bufr_translation[i]) == dict:
                        try:    subkey = list(self.bufr_translation[i])
                        except: continue
                        if type(subkey[0]) == float:
                            if subkey[0] < 0:   self.depth_keys.add(i)
                            elif subkey[0] > 0: self.height_keys.add(i)

                self.unit_keys = frozenset(self.unit_keys)

            case "se" | "pd" | "fl":
                
                self.bufr_obs_keys          = frozenset(self.bufr_translation_keys) 
                
                if script != "se": self.fixed_duration_keys = set()

                for i in self.bufr_translation_keys:
                    if type(self.bufr_translation[i]) == dict:

                        try:    subkey = list(self.bufr_translation[i])[0]
                        except: continue

                        if script != "se" and self.bufr_translation[i][subkey][1] is not None:
                            self.fixed_duration_keys.add(i)

                        if type(subkey) == float:
                            if subkey   < 0: self.depth_keys.add(i)
                            elif subkey > 0: self.height_keys.add(i)

                    elif script != "se" and type(self.bufr_translation[i]) == list:
                        if self.bufr_translation[i][1] is not None:
                            self.fixed_duration_keys.add(i)

                if script != "se": self.fixed_duration_keys = frozenset(self.fixed_duration_keys)

        if script == "pd":
            self.ww             = "presentWeather"
            self.rr             = "totalPrecipitationOrTotalWaterEquivalent"
            self.wmo            = "WMO_station_id"
            self.dt             = "data_datetime"
            self.required_keys  = frozenset({self.wmo, self.dt})
            self.relevant_keys  = frozenset( self.bufr_obs_keys | self.required_keys | self.modifier_keys | {self.obs_sequence,self.replication} )


        # union of both will be used later
        self.height_keys, self.depth_keys   = frozenset(self.height_keys), frozenset(self.depth_keys)
        self.height_depth_keys              = frozenset( self.height_keys | self.depth_keys )

    ### class lambda functions
    to_datetime = lambda meta    : dt(meta["year"], meta["month"], meta["day"], meta["hour"], meta["minute"])
    clear       = lambda keyname : str( re.sub( r"#[0-9]+#", '', keyname ) )
    number      = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
    to_key      = lambda key,num : "#{num}#{key}"


    ### class functions
    def translate_key_us( self, key, value, duration, h=None, unit=None ):

        key_db = self.bufr_translation[key]

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
        else: self.bufr_translation[key]

        # add units + scale conversion
        value = float(value) * key_db[2] + key_db[3]

        # if we already got a timePeriod from BUFR we dont need to translate it with the yaml dict
        if not duration: duration = key_db[1]
        if duration is None: duration = "NULL"

        return key_db[0], value, duration


    def translate_key_se( self, key, value, duration, h=None ):

        key_db = self.bufr_translation[key]

        if key_db is None: return None, None, None
        if h is not None: # we are looking for a specific height/depth key, key_db is a dict now
            try:    key_db = key_db[h]
            except  KeyError:
                print("height ERROR")
                print(key, value, h)
                return None, None, None
        else: self.bufr_translation[key]

        # add units + scale conversion
        value = float(value) * key_db[2] + key_db[3]

        # if we already got a timePeriod from BUFR we dont need to translate it with the yaml dict
        if not duration: duration = key_db[1]
        if duration is None: duration = "NULL"

        return key_db[0], value, duration


    def translate_key_pd( self, key, value, duration, h=None ):

        key_db = self.bufr_translation[key]

        if key_db is None: return None, None, None
        if h is not None: # we are looking for a specific height/depth key, key_db is a dict now
            try:    key_db = key_db[h]
            except  KeyError:
                print("height ERROR")
                print(key, value, h)
                return None, None, None
        else: self.bufr_translation[key]

        # add units + scale conversion
        value = float(value) * key_db[2] + key_db[3]

        # if we already got a timePeriod from BUFR we dont need to translate it with the yaml dict
        if not duration or key in self.fixed_duration_keys: duration = key_db[1]
        if duration is None: duration = "NULL"

        return key_db[0], value, duration


    # version with units (decode_bufr_us.py)
    def convert_keys_us(self, obs, dataset, modifier_keys, verbose=None):
        
        if verbose is None: verbose = self.verbose

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
                        elif key == "verticalSignificanceSurfaceObservations": vertical_sigf = self.bufr_flags[key][val_obs]
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

                                element, val_db, duration = self.translate_key_us(bufr_translation, key, val_obs, duration, h=h)

                            elif key in unit_keys:
                                if key == "heightOfBaseOfCloud":
                                    # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                    if val_obs < cloud_ceiling:
                                        cloud_ceiling   = copy(val_obs)
                                        unit_ceiling    = data[2]

                                    # also we want to save all unique cloud levels (base heights) which where observed
                                    cloud_bases.add(val_obs)
                                    continue

                                else: element,val_db,duration = self.translate_key_us(bufr_translation, key, val_obs, duration, unit=data[2])

                            elif key == "cloudCoverTotal":
                                cloud_cover = copy(val_obs)
                                element, val_db, duration = self.translate_key_us(bufr_translation, key, cloud_cover, duration)
                                if dataset in {"DWD","test"}: val_db = int(val_db)

                            elif key == "cloudAmount":
                                if cloud_cover is None: cloud_amounts.add( val_obs )

                                element, val_db, duration = self.translate_key_us(bufr_translation, key, val_obs, duration, h=vertical_sigf )
                                if not vertical_sigf: continue

                            else: element, val_db, duration = self.translate_key_us(bufr_translation, key, val_obs, duration)
                            if element is not None:
                                obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                            else: print(f"element is None for key: {key}, value: {val_obs}")

                    if cloud_ceiling < float("inf"):
                        key = "heightOfBaseOfCloud"
                        element, val_db, duration = self.translate_key_us(bufr_translation, key, cloud_ceiling, duration, unit=unit_ceiling )
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = self.translate_key_us(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = self.translate_key_us(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

        return obs_db


    # skip extra key attributes (like units) version
    def convert_keys_se(self, obs, dataset, verbose=None):
        
        if verbose is None: verbose = self.verbose
        
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
                    if len(obs[file][location][datetime]) > 1 and obs[file][location][datetime][-1][0] in self.modifier_keys:
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
                            vertical_sigf = self.bufr_flags[key][val_obs]
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

                                element, val_db, duration = self.translate_key_se(bufr_translation, key, val_obs, duration, h=h)

                            elif key == "heightOfBaseOfCloud":
                                    # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                    if val_obs < cloud_ceiling:
                                        cloud_ceiling   = copy(val_obs)

                                    # also we want to save all unique cloud levels (base heights) which where observed
                                    cloud_bases.add(val_obs)
                                    continue

                            elif key == "cloudCoverTotal":
                                cloud_cover = copy(val_obs)
                                element, val_db, duration = self.translate_key_se(bufr_translation, key, cloud_cover, duration)
                                if dataset in {"DWD","test"}: val_db = int(val_db)

                            elif key == "cloudAmount":
                                if cloud_cover is None: cloud_amounts.add( val_obs )

                                element, val_db, duration = self.translate_key_se(bufr_translation, key, val_obs, duration, h=vertical_sigf )
                                if not vertical_sigf: continue

                            else: element, val_db, duration = self.translate_key_se(bufr_translation, key, val_obs, duration)
                            if element is not None:
                                obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                            else: print(f"element is None for key: {key}, value: {val_obs}")

                    if cloud_ceiling < float("inf"):
                        key = "heightOfBaseOfCloud"
                        element, val_db, duration = self.translate_key_se(bufr_translation, key, cloud_ceiling, duration )
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = self.translate_key_se(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = self.translate_key_se(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

        #if verbose: print(obs_db)
        print(counter)
        return obs_db


    # pdbufr version
    def convert_keys_pd( self, obs, dataset, verbose=None ):

        if verbose is None: verbose = self.verbose

        time_periods = self.bufr_translation["timePeriod"]

        if self.debug: print(obs)
        obs_db = {}
        #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

        for file in obs:

            for location in obs[file]:

                if location not in obs_db: obs_db[location] = set()

                for datetime in obs[file][location]:

                    duration_obs    = ""
                    vertical_sigf   = 0
                    clouds_present  = False
                    cloud_cover     = None
                    cloud_ceiling   = float("inf")
                    cloud_amounts, cloud_bases  = set(), set()
                    sensor_height, sensor_depth = None, None

                    for time_period in obs[file][location][datetime]:

                        try:
                            if len(obs[file][location][datetime][time_period]) > 0 and obs[file][location][datetime][time_period][1][0] in self.modifier_keys:
                                del obs[file][location][datetime][time_period][1]
                            if len(obs[file][location][datetime][time_period]) == 0: continue
                        except: continue

                        try:
                            if time_period: duration_obs = time_periods[time_period]
                        except: continue

                        datetime_db     = datetime.to_pydatetime()
                        cor             = 0

                        for data in obs[file][location][datetime][time_period]:

                            key, val_obs = data[0], data[1]

                            #if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                            #else:                           datetime_db = copy(datetime)

                            if key == self.vertical_signf:
                                vertical_sigf = self.bufr_flags[key][val_obs]
                            elif key == self.sensor_height:
                                sensor_height = float(val_obs)
                            elif key == self.sensor_depth:
                                sensor_depth  = float(val_obs) * (-1)
                            elif key == self.obs_sequence:
                                cor = int(val_obs)
                            else:
                                if key in self.height_depth_keys:
                                    if key == "soilTemperature":
                                        h = copy(sensor_depth)
                                        if not h or h > 0: h = -0.05
                                    else: # we take 1m temperature obs as 2m as well
                                        h = copy(sensor_height)
                                        print(h, type(h))
                                        if not h or h >= 1: h = 2.0

                                    element, val_db, duration = self.translate_key_pd(key, val_obs, duration_obs, h)
                                elif key in {"heightOfBaseOfCloud","cloudCoverTotal","cloudAmount"}:
                                    clouds_present = True
                                    if key == "heightOfBaseOfCloud":
                                        # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                        if val_obs < cloud_ceiling: cloud_ceiling = copy(val_obs)

                                        # also we want to save all unique cloud levels (base heights) which where observed
                                        cloud_bases.add(val_obs); continue

                                    elif key == "cloudCoverTotal":
                                        cloud_cover = copy(val_obs)
                                        element, val_db, duration = self.translate_key_pd(key, cloud_cover, duration_obs)
                                        # in DWD BUFRs cloudCoverTotal is effectively only in octas, even though coded in percent
                                        if dataset in {"DWD","test"}: val_db = int(val_db)

                                    #elif key == "cloudAmount":
                                    else:
                                        cloud_amounts.add( val_obs )
                                        if not vertical_sigf: continue
                                        element, val_db, duration = self.translate_key_pd(key, val_obs, duration_obs, vertical_sigf)

                                else: element, val_db, duration = self.translate_key_pd(key, val_obs, duration_obs)
                                if element is not None:
                                    obs_db[location].add( ( file, datetime_db, duration, element, val_db, cor ) )
                                else: print(f"element is None for key: {key}, value: {val_obs}")


                    if clouds_present:
                        if cloud_ceiling < float("inf"):
                            key = "heightOfBaseOfCloud"
                            element, val_db, duration = self.translate_key_pd(key, cloud_ceiling, duration_obs )
                            obs_db[location].add( ( file, datetime_db, duration, element, val_db, cor ) )

                        if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                            element, val_db, duration = self.translate_key_pd("cloudAmount", max(cloud_amounts), duration_obs, 0)
                            obs_db[location].add( ( file, datetime_db, duration, element, val_db, cor ) )

                        if cloud_bases:
                            cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                            for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                                element, val_db, duration = self.translate_key_pd("cloudBase", cloud_base, duration_obs, i+1)
                                obs_db[location].add( ( file, datetime_db, duration, element, val_db, cor ) )
                            cloud_bases = set()


        return obs_db
