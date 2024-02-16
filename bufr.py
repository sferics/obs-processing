import sys
import re
import eccodes as ec
import numpy as np
from copy import copy
from datetime import datetime as dt, timedelta as td
import global_functions as gf
import global_variables as gv

# general functions and constants

to_code = lambda integer : str(integer).rjust(6, "0")

def to_wmo( block, station, add_zero=True ):
    """
    Get WMO ID from blockNumber and stationNumber
    """
    location = station + block * 1000
    location = str(location).rjust(5,"0")

    if add_zero: location += "0"

    return location


class BufrClass:
   
    # general function definitions and constants which are always available, even before class init
    # https://stackoverflow.com/questions/45268794/calling-a-class-function-without-triggering-the-init
    #TODO consider whether this is necessary???
    @staticmethod
    def to_code(integer):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        return str(integer).rjust(6, "0")
    
    @staticmethod
    def to_wmo( block, station, add_zero=True ):
        """
        Parameter:
        ----------

        Notes:
        ------
        Get WMO ID from blockNumber and stationNumber

        Return:
        -------

        """
        location = station + block * 1000
        location = str(location).rjust(5,"0")

        if add_zero: location += "0"

        return location


    def __init__(self, config={"bufr_translation":"bufr_translation", "bufr_flags":"bufr_flags", "mode":"dev", "output_path":"~/data/stations"}, script="us"):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        # default settings values in case they are not present in the config dict
        self.verbose    = False
        self.traceback  = False
        self.stations   = ["wmo"]
        
        # parse all keys and values of config dict into namespace, a bit like in database.py
        for i in config:
            setattr(self, i, config[i])
            if self.verbose: print( i, "=", config[i] )

        # check for mandatory class attributes
        mandatory = ("bufr_translation", "bufr_flags", "mode", "output")
        for attr in mandatory:
            print(attr)
            assert( hasattr(self, attr) )

        if "log_level" in config and config["log_level"] in gv.log_levels: 
            self.log_level = config["log_level"]
        else: self.log_level = "NOTSET"

        self.log = gf.get_logger( self.__class__.__name__, self.log_level )

        if not hasattr(self, "max_files"): self.max_files = 0

        # common key names for BUFR obs decoding scrips
        self.YMD                = frozenset( {"year", "month", "day"} )
        # timePeriod is very frequently used so we shorten it up a lot
        self.tp                 = "timePeriod"
        self.obs_sequence       = "observationSequenceNumber"
        self.replication        = "delayedDescriptorReplicationFactor"
        self.ext_replication    = "extendedDelayedDescriptorReplicationFactor"
        self.short_replication  = "shortDelayedDescriptorReplicationFactor"
        self.replication_keys   = frozenset( {self.replication, self.ext_replication, self.short_replication} )
        #"delayedDescriptorAndDataRepetitionFactor", "extendedDelayedDescriptorAndDataRepetitionFactor"} )
        self.sensor_height      = "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform"
        self.sensor_depth       = "depthBelowLandSurface"
        self.vertical_signf     = "verticalSignificanceSurfaceObservations"
        self.modifier_keys      = frozenset( {self.sensor_height, self.sensor_depth, self.vertical_signf, self.tp} )
        
        self.WMO                = frozenset( {"stationNumber", "blockNumber"} )
        self.station_keys       = frozenset( {"shortStationName", "stationNumber", "blockNumber"} )
        self.time_keys          = ("year", "month", "day", "hour", "minute")
        self.time_keys_day      = self.time_keys[:-2]
        self.time_keys_hour     = self.time_keys[:-1]
        self.set_time_keys      = frozenset(self.time_keys)
        self.set_time_keys_hour = frozenset(self.time_keys_hour)

        self.typical_time_keys  = ( "typical" + i.capitalize() for i in self.time_keys )
        self.typical_datetime   = frozenset( {"typicalDate","typicalTime"} )
        self.typical_keys       = frozenset( set(self.typical_time_keys) | self.typical_datetime )

        # MISSING values in ECCODES are:     {2147483647,            -1e+100}
        self.null_vals          = frozenset( {ec.CODES_MISSING_LONG, ec.CODES_MISSING_DOUBLE} )
        self.meta_ignore_vals   = frozenset( {"null", "NULL", "MISSING", "XXXX", " ", ""} )
        self.meta_null_vals     = frozenset( self.null_vals | self.meta_ignore_vals )
        
        self.skip_status        = frozenset( {"locked_.", "error", "empty", "parsed"} )

        # parse the BUFR translation and bufr flags files into dictionaries
        self.bufr_translation   = gf.read_yaml( self.bufr_translation )
        self.bufr_flags         = gf.read_yaml( self.bufr_flags )

        # remove unit translations (first 5 keys)
        self.bufr_translation_keys  = tuple(self.bufr_translation)[5:]
        
        # get special types of keys
        self.depth_keys, self.height_keys = set(), set()

        
        match script:
            
            case "00":
                
                self.unit_keys = set()
                
                for i in self.bufr_translation_keys:
                    if type(self.bufr_translation[i]) == list and type(self.bufr_translation[i][0]) == dict:
                        self.unit_keys.add(i)
                    elif type(self.bufr_translation[i]) == dict:
                        try:    subkey = list(self.bufr_translation[i])
                        except: continue
                        if type(subkey[0]) == float:
                            if subkey[0] < 0:
                                self.depth_keys.add(i)
                            elif subkey[0] > 0:
                                self.height_keys.add(i)

                self.unit_keys = frozenset(self.unit_keys)
                self.obs_time_keys  = frozenset( set(self.bufr_translation_keys) | set(self.time_keys) - {"cloudBase"} )
                self.relevant_keys  = frozenset( self.obs_time_keys | self.station_keys | self.typical_keys )

            case "us" | "ex":
                
                self.bufr_translation_codes = {}
                self.depth_codes, self.height_codes = set(), set()

                for i in self.bufr_translation_keys:
                    
                    if type(self.bufr_translation[i]) == dict:
                            
                        for j in self.bufr_translation[i]:
                            
                            self.bufr_translation_codes[j] = self.bufr_translation[i][j]

                            try:    subkey = list(self.bufr_translation[i][j])
                            except: continue
                            
                            if type(subkey[0]) == float:
                                if subkey[0] < 0:
                                    self.depth_keys.add(i)
                                    self.depth_codes.add(j)
                                elif subkey[0] > 0:
                                    self.height_keys.add(i)
                                    self.height_codes.add(j)

                self.bufr_translation = copy(self.bufr_translation_codes)

                self.obs_time_keys  = frozenset( set(self.bufr_translation_keys) | set(self.time_keys) - {"cloudBase"} )
                self.relevant_keys  = frozenset( self.obs_time_keys | self.station_keys | self.typical_keys )

                # all codes which contain timePeriod information for our synoptic purposes
                self.tp_codes       = frozenset( {4023, 4024, 4025} ) # d, h, min
                self.tp_range       = range(4023, 4026)

                # codes which alter height/depth of sensor
                self.height_depth_codes = frozenset( self.height_codes | self.depth_codes )
                # all codes which modify the following keys duration, height, depth and so on
                self.modifier_codes = frozenset( self.tp_codes | {1023, 7032, 7061, 8002, 31000, 31001, 31002} )
                #TODO maybe add 31011, 31012 (not used by DWD but maybe other providers do use them)

                if script in {"ex", "us"}:
                    self.datetime_codes = {
                        4001 : "year",  # XXXX
                        4002 : "month", # XX
                        4003 : "day",   # XX
                        4004 : "hour",  # XX
                        4005 : "minute" # XX
                    }
                    self.station_codes  = frozenset( {1001, 1002} ) # 1018
                    
                    if hasattr(self, "bufr_sequences"):
                        self.bufr_sequences = gf.read_yaml( self.bufr_sequences )
                    else: self.bufr_sequences = gf.read_yaml( "bufr_sequences" )

                    self.sequence_range = range(min(self.bufr_sequences), max(self.bufr_sequences))
                    self.scale_increase = 0

                    self.scale_alter = {
                        202000 : 0, # reset scale
                        202129 : 1, # temporarily increase scale by 1 digit
                    }

                    self.repl_codes = frozenset( {31000, 31001, 31002} )

                    # all code relevant for extraction of expanded descriptors
                    self.relevant_codes = frozenset( self.modifier_codes | set(self.bufr_translation) | set(self.scale_alter) - self.repl_codes )
        
                    self.size_alter = {
                        201000 : 0, # reset size
                        201132 : 4, # temporarily increase data size by 4 bits
                    }
                    
                    self.scale_size_alter = frozenset(set(self.scale_alter) | set(self.size_alter))

                    self.repl_range     = range(101000, 131000) # range of repeated elements
                    self.repl_seq_range = range(131000, 132000) # repeat next sequence (999 times)
                    self.repl_info      = frozenset( tuple(self.repl_codes) + tuple(self.repl_range) + tuple(self.repl_seq_range) )

            case "se" | "pd" | "pl" | "fl" | "gt":
                
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
                
                self.bufr_obs_keys = frozenset( set(self.bufr_translation_keys) - {"cloudBase"} )

                if script in {"pd", "pl", "gt"}:
                    self.ww             = "presentWeather"
                    self.rr             = "totalPrecipitationOrTotalWaterEquivalent"
                    self.wmo            = "WMO_station_id"
                    self.dt             = "data_datetime"
                    self.required_keys  = frozenset( {self.wmo, self.dt} )
                    self.relevant_keys  = frozenset( self.bufr_obs_keys | self.required_keys | self.modifier_keys | {self.obs_sequence} | self.replication_keys )
                    
                    if script in {"pl", "gt"}:
                        self.ignore_keys    = frozenset( self.required_keys | self.replication_keys | {self.tp} )
                        self.obs_list_keys  = frozenset( self.relevant_keys - self.ignore_keys )

                elif script == "se":
                    self.bufr_mod_keys  = frozenset( set(self.bufr_translation_keys) | self.modifier_keys | {self.obs_sequence} )
                    self.relevant_keys  = frozenset( self.bufr_mod_keys | self.station_keys | self.typical_keys | set(self.time_keys) )
                    self.time_keys_set  = frozenset( self.time_keys )
        
        # we currently don't need these seperate key groups, outcommented for future use
        #self.height_keys, self.depth_keys   = frozenset(self.height_keys), frozenset(self.depth_keys)
        # union of both will be used later
        self.height_depth_keys              = frozenset( self.height_keys | self.depth_keys )


    ### class lambda functions
    clear       = lambda self, keyname  : str( re.sub( r"#[0-9]+#", '', keyname ) )
    number      = lambda self, keyname  : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
    to_key      = lambda self, key, num : "#{num}#{key}"


    ### class functions
    def translate_key_00( self, key, value, duration, h=None, unit=None ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
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

        # if we already got a timePeriod from BUFR we dont need to translate it with the dict
        if duration is None:    duration = "NULL"
        elif not duration:      duration = key_db[1]

        return key_db[0], value, duration


    def translate_key_se( self, key, value, duration, h=None ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
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

        # if we already got a timePeriod from BUFR we dont need to translate it with the dict
        if duration is None:    duration = "NULL"
        elif not duration:      duration = key_db[1]

        return key_db[0], value, duration


    def translate_key_us( self, key, value, duration, h=None ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        key_db = self.bufr_translation[key]

        if key_db is None: return None, None, None, None
        if h is not None: # we are looking for a specific height/depth key, key_db is a dict now
            try:    key_db = key_db[h]
            except  KeyError:
                print("height ERROR")
                print(key, value, h)
                return None, None, None, None
        else: self.bufr_translation[key]

        # add units + scale conversion
        value = float(value) * key_db[2] + key_db[3]
        # devide scale by unit conversion factor and add scale increase if present
        scale = ( 10 ** key_db[4] ) / key_db[2] + self.scale_increase


        # if we already got a timePeriod from BUFR we dont need to translate it with the dict
        if duration is None:    duration = "NULL"
        elif not duration:      duration = key_db[1]

        return key_db[0], value, duration, scale


    def translate_key_pd( self, key, value, duration, h=None ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
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

        # if we already got a timePeriod from BUFR we dont need to translate it with the dict
        if duration is None or key in self.fixed_duration_keys:
            duration = key_db[1]
        if duration is None: duration = ""

        return key_db[0], value, duration


    # version with units, without scale (decode_bufr_00.py)
    def convert_keys_00(self, obs, dataset, verbose=None):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
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
                    vertical_signf  = 0
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
                            duration    = copy(val_obs)
                            datetime_db = copy(datetime)
                        elif key == self.vertical_signf: vertical_signf = self.bufr_flags[key][val_obs]
                        elif key == self.sensor_height:
                            sensor_height = float(val_obs)
                        elif key == self.sensor_depth:
                            sensor_depth  = float(val_obs) * (-1)
                        else:
                            if key in self.height_depth_keys:
                                if key == "soilTemperature":
                                    h = copy(sensor_depth)
                                    if not h or h > 0: h = -0.05
                                else:
                                    h = copy(sensor_height)
                                    if not h or h > 1: h = 2.0

                                element, val_db, duration = self.translate_key_00(bufr_translation, key, val_obs, duration, h=h)

                            elif key in unit_keys:
                                if key == "heightOfBaseOfCloud":
                                    # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                    if val_obs < cloud_ceiling:
                                        cloud_ceiling   = copy(val_obs)
                                        unit_ceiling    = data[2]

                                    # also we want to save all unique cloud levels (base heights) which where observed
                                    cloud_bases.add(val_obs)
                                    continue

                                else: element,val_db,duration = self.translate_key_00(bufr_translation, key, val_obs, duration, unit=data[2])

                            elif key == "cloudCoverTotal":
                                cloud_cover = copy(val_obs)
                                element, val_db, duration = self.translate_key_00(bufr_translation, key, cloud_cover, duration)
                                if dataset in {"DWD","test"}: val_db = int(val_db)

                            elif key == "cloudAmount":
                                if cloud_cover is None: cloud_amounts.add( val_obs )

                                element, val_db, duration = self.translate_key_00(bufr_translation, key, val_obs, duration, h=vertical_signf )
                                if not vertical_signf: continue

                            else: element, val_db, duration = self.translate_key_00(bufr_translation, key, val_obs, duration)
                            if element is not None:
                                obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )
                            else: print(f"element is None for key: {key}, value: {val_obs}")

                    if cloud_ceiling < float("inf"):
                        key = "heightOfBaseOfCloud"
                        element, val_db, duration = self.translate_key_00(bufr_translation, key, cloud_ceiling, duration, unit=unit_ceiling )
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = self.translate_key_00(bufr_translation, "cloudAmount", max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = self.translate_key_00(bufr_translation, "cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (dataset, file, datetime_db, duration, element, val_db) )

        return obs_db


    # version with units and scale (uses codes instead of keys) + expanded keys + values
    def convert_keys_us( self, obs, dataset, verbose=False):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        #time_periods = self.bufr_translation["timePeriod"]

        if verbose: print(obs)
        obs_db = {}
        #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

        for file in obs:
            for location in obs[file]:
                if location not in obs_db: obs_db[location] = set()

                for datetime in obs[file][location]:
                    if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                    else:                           datetime_db = copy(datetime)

                    cor             = 0
                    duration        = ""
                    vertical_sigf   = 0
                    cloud_cover     = None
                    cloud_ceiling   = float("inf")
                    cloud_amounts, cloud_bases  = set(), set()
                    sensor_height, sensor_depth = None, None

                    # delete the last element of the list if it's a modifier code; it can go modify the weather of Island
                    if len(obs[file][location][datetime]) > 1 and obs[file][location][datetime][-1][0] in self.modifier_codes:
                        del obs[file][location][datetime][-1]

                    for data in obs[file][location][datetime]:

                        code, val_obs = data[0], data[1]
                        
                        if code == 1023: # observationSequenceNumber
                            cor = copy(val_obs)
                        elif code in self.tp_codes:
                            try:    duration    = self.bufr_translation[code][val_obs]
                            except: duration    = "" # TODO or continue to skip unknown duration?
                            datetime_db = copy(datetime)
                        elif code == 8002:
                            try:    vertical_sigf = self.bufr_flags[code][int(val_obs)]
                            except KeyError:
                                print(code, val_obs)
                                print(data)
                        elif code == 7032:
                            sensor_height = float(val_obs)
                            continue
                        elif code == 7061:
                            sensor_depth  = float(val_obs) * (-1)
                            continue
                        elif code in self.scale_alter:
                            self.scale_increase = self.scale_alter[code]
                        else:
                            if code in self.height_depth_codes:
                                if code in self.depth_codes: # soilTemperature
                                    h = copy(sensor_depth)
                                    if not h or h > 0: h = -0.05
                                else: # we only accept temperatures measured at >= 1m as T2m
                                    h = copy(sensor_height)
                                    if not h or h >= 1: h = 2.0

                                element, val_db, duration, scale = self.translate_key_us(code, val_obs, duration, h=h)

                            elif code in {20013, 20092}:
                                # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                if val_obs < cloud_ceiling:
                                    cloud_ceiling   = copy(val_obs)
                                    code_ceiling    = copy(code)

                                # also we want to save all unique cloud levels (base heights) which where observed
                                cloud_bases.add(val_obs)
                                continue

                            elif code == 20010:
                                cloud_cover = copy(val_obs)
                                element, val_db, duration, scale = self.translate_key_us(code, cloud_cover, duration)
                                if dataset in {"DWD","test"}: val_db = int(val_db)

                            elif code == 20011:
                                if cloud_cover is None: cloud_amounts.add( val_obs )

                                element, val_db, duration, scale = self.translate_key_us(code, val_obs, duration, h=vertical_sigf )
                                if not vertical_sigf: continue

                            else: element, val_db, duration, scale = self.translate_key_us(code, val_obs, duration)
                            if element is not None:
                                obs_db[location].add( (file, datetime_db, duration, element, val_db, cor, scale) )
                            else: print(f"element is None for code: {code}, value: {val_obs}")

                    if cloud_ceiling < float("inf"):
                        code = copy(code_ceiling)
                        element, val_db, duration, scale = self.translate_key_us(code, cloud_ceiling, duration )
                        obs_db[location].add( (file, datetime_db, duration, element, val_db, cor, scale) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration, scale = self.translate_key_us(20011, max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (file, datetime_db, duration, element, val_db, cor, scale) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration, scale = self.translate_key_us("cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (file, datetime_db, duration, element, val_db, cor, scale) )

        return obs_db


    # skip extra key attributes (like units) version
    def convert_keys_se(self, obs, dataset, verbose=None):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        time_periods = self.bufr_translation["timePeriod"]

        if verbose is None: verbose = self.verbose
        
        if verbose: print(obs)
        obs_db = {}
        #obs_db = shelve.open("shelves/obs_db.shelve", writeback=True)

        counter = {}

        for file in obs:

            for location in obs[file]:

                if location not in obs_db: obs_db[location] = set()

                for datetime in obs[file][location]:

                    if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                    else:                           datetime_db = copy(datetime)
                    
                    cor             = 0
                    duration        = ""
                    offset          = 0
                    vertical_signf  = 0
                    cloud_cover     = None
                    cloud_ceiling   = float("inf")
                    cloud_amounts, cloud_bases  = set(), set()
                    sensor_height, sensor_depth = None, None

                    # delete the last element of the list if it's a modifier key; it can go modify the weather of Island
                    if len(obs[file][location][datetime]) > 1 and obs[file][location][datetime][-1][0] in self.modifier_keys:
                        del obs[file][location][datetime][-1]

                    for data in obs[file][location][datetime]:

                        key, val_obs = data[0], data[1]

                        if key == self.obs_sequence:
                            cor = int(val_obs)
                        elif key == "timePeriod":
                            try:    counter[key] += 1
                            except: counter[key] = 0
                            duration = time_periods[val_obs]
                            """
                            if val_obs == -10:
                                offset += 10
                                datetime_db -= td(minutes=offset)
                            else:               datetime_db = copy(datetime)
                            """
                            datetime_db = copy(datetime)
                        elif key == self.sensor_height:
                            try:    counter[key] += 1
                            except: counter[key] = 0
                            sensor_height = float(val_obs)
                        elif key == self.vertical_signf:
                            try:    counter[key] += 1
                            except: counter[key] = 0
                            vertical_signf = self.bufr_flags[key][val_obs]
                        elif key == self.sensor_depth:
                            try:    counter[key] += 1
                            except: counter[key] = 0
                            sensor_depth  = float(val_obs) * (-1)
                        else:
                            if key in self.height_depth_keys:
                                if key == "soilTemperature":
                                    h = copy(sensor_depth)
                                    if not h or h >= 0: h = -0.05
                                else:
                                    h = copy(sensor_height)
                                    if not h or h >= 1: h = 2.0

                                element, val_db, duration = self.translate_key_se(key, val_obs, duration, h=h)

                            elif key == "heightOfBaseOfCloud":
                                # we are first and foremost interested in the cloud base of the lowest cloud(ceiling)
                                if val_obs < cloud_ceiling:
                                    cloud_ceiling   = copy(val_obs)

                                # also we want to save all unique cloud levels (base heights) which where observed
                                cloud_bases.add(val_obs)
                                continue

                            elif key == "cloudCoverTotal":
                                cloud_cover = copy(val_obs)
                                element, val_db, duration = self.translate_key_se(key, cloud_cover, duration)
                                if dataset in {"DWD","test"}: val_db = int(val_db)

                            elif key == "cloudAmount":
                                if cloud_cover is None: cloud_amounts.add( val_obs )

                                element, val_db, duration = self.translate_key_se(key, val_obs, duration, h=vertical_signf )
                                if not vertical_signf: continue

                            else: element, val_db, duration = self.translate_key_se(key, val_obs, duration)
                            if element is not None:
                                obs_db[location].add( (file, datetime_db, duration, element, val_db, cor) )
                            else: print(f"element is None for key: {key}, value: {val_obs}")

                    if cloud_ceiling < float("inf"):
                        key = "heightOfBaseOfCloud"
                        element, val_db, duration = self.translate_key_se(key, cloud_ceiling, duration )
                        obs_db[location].add( (file, datetime_db, duration, element, val_db, cor) )

                    if cloud_cover is None and cloud_amounts: # we prefer cloud cover over cloud amount because it's in %
                        element, val_db, duration = self.translate_key_se("cloudAmount", max(cloud_amounts), duration, h=0)
                        obs_db[location].add( (file, datetime_db, duration, element, val_db, cor) )

                    if cloud_bases:
                        cloud_bases = sorted(cloud_bases)[:4] # convert set into a sorted list (lowest to highest level)
                        for i, cloud_base in enumerate(cloud_bases): # get all the cloud base heights from 1-4
                            element, val_db, duration = self.translate_key_se("cloudBase", cloud_base, duration, h=i+1)
                            obs_db[location].add( (file, datetime_db, duration, element, val_db, cor) )

        #if verbose: print(obs_db)
        #print(counter)
        return obs_db


    # pdbufr version
    def convert_keys_pd( self, obs, dataset, convert_datetime=True, verbose=None ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
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
                    vertical_signf  = 0
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
                        
                        if convert_datetime:
                            datetime_db = datetime.to_pydatetime()
                        else: datetime_db = copy(datetime)
                        
                        cor = 0

                        for data in obs[file][location][datetime][time_period]:

                            key, val_obs = data[0], data[1]

                            #if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                            #else:                           datetime_db = copy(datetime)

                            if key == self.vertical_signf:
                                vertical_signf = self.bufr_flags[key][val_obs]
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
                                        if not vertical_signf: continue
                                        element, val_db, duration = self.translate_key_pd(key, val_obs, duration_obs, vertical_signf)

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
