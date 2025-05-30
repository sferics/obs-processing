import sys
import re
import eccodes as ec
import numpy as np
from copy import copy
from datetime import datetime as dt, timedelta as td
from config import ConfigClass
import global_functions as gf
import global_variables as gv

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

    @staticmethod
    def get_clear_key(key):
        """
        """
        regex = r"#[0-9]+#"
        return str( re.sub(regex, "". key) )
    
    @staticmethod
    def get_number(key):
        """
        """
        regex = r"#[A-Za-z0-9]+"
        return int( re.sub(regex, "", key[1:]) )
    
    @staticmethod
    def to_key(self, clear_key, number):
        """
        """
        return f"#{number}#{clear_key}"

    @classmethod
    def get_code(msg, key=None, clear_key=None, number=None, datatype=None):
        """
        """
        if clear_key and number:
            key = self.to_key(clear_key, number)
        if datatype == "native":
            datatype = ec.codes_get_native_type(msg, key)
        match datatype:
            case "int" | "long":
                return ec.codes_get_long(msg, key)
            case "float" | "double":
                return ec.codes_get_double(msg, key)
            case "str" | "string":
                return ec.codes_get_string(msg, key)
            case "array":
                return ec.codes_get_array(msg, key)
            case _: # if datatype is None or unknown
                return ec.codes_get(msg, key)


    @classmethod
    def __init__(self, cf: ConfigClass, source: str="extra", approach: str="gt"):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        self.source = source

        # initialize bufr class (contains all bufr specifics contants and settings)
        config = cf.general | cf.bufr | cf.script

        # try to apply the source specific config, if it does exists, else skip
        try:    config = config | cf.sources[source]
        except: pass
        
        # default settings values in case they are not present in the config dict
        self.verbose    = False
        self.traceback  = False
        self.stations   = ("wmo",)
        
        # parse all keys and values of config dict into namespace, a bit like in database.py
        for i in config:
            setattr(self, i, config[i])
            if self.verbose: print( i, "=", config[i] )
        
        # make config accessible as class object
        self.config = config

        # check for mandatory class attributes
        mandatory = ("mode", "output", "approach")
        for attr in mandatory:
            assert( hasattr(self, attr) )

        if "log_level" in config and config["log_level"] in gv.log_levels: 
            self.log_level = config["log_level"]
        else: self.log_level = "NOTSET"

        self.log = gf.get_logger( self.__class__.__name__, self.log_level )

        if not hasattr(self, "max_files"): self.max_files = 0

        # common key names for BUFR obs decoding scrips
        self.YMD                = {"year", "month", "day"}
        # timePeriod is very frequently used so we shorten it up a lot
        self.tp                 = "timePeriod"
        self.obs_sequence       = "observationSequenceNumber"
        self.replication        = "delayedDescriptorReplicationFactor"
        self.ext_replication    = "extendedDelayedDescriptorReplicationFactor"
        self.short_replication  = "shortDelayedDescriptorReplicationFactor"
        self.replication_keys   = {self.replication, self.ext_replication, self.short_replication}
        #"delayedDescriptorAndDataRepetitionFactor", "extendedDelayedDescriptorAndDataRepetitionFactor"} )
        self.sensor_height      = "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform"
        self.sensor_depth       = "depthBelowLandSurface"
        self.vertical_signf     = "verticalSignificanceSurfaceObservations"
        self.modifier_keys      = {self.sensor_height, self.sensor_depth, self.vertical_signf, self.tp}
        
        self.WMO                = {"stationNumber", "blockNumber"}
        self.station_keys       = {"shortStationName", "stationNumber", "blockNumber"}
        self.time_keys          = ("year", "month", "day", "hour", "minute")
        self.time_keys_day      = self.time_keys[:-2]
        self.time_keys_hour     = self.time_keys[:-1]
        self.set_time_keys      = set(self.time_keys)
        self.set_time_keys_hour = set(self.time_keys_hour)

        self.typical_time_keys  = ( "typical" + i.capitalize() for i in self.time_keys )
        self.typical_datetime   = {"typicalDate", "typicalTime"}
        self.typical_keys       = set(self.typical_time_keys) | self.typical_datetime

        # MISSING values in ECCODES are:     {2147483647,            -1e+100}
        self.null_vals          = {ec.CODES_MISSING_LONG, ec.CODES_MISSING_DOUBLE}
        self.meta_ignore_vals   = {"null", "NULL", "MISSING", "XXXX", " ", ""}
        self.meta_null_vals     = self.null_vals | self.meta_ignore_vals
        
        # these durations will never be changed by a timePeriod value
        self.fixed_durations    = {"0s", "1s", "1min"}
        # skip the following file statuses when processing without -r/--redo flag
        self.skip_status        = {"locked_.", "error", "empty", "parsed"}

        # parse the BUFR translation and bufr flags files into dictionaries
        self.bufr_translation   = gf.read_yaml( "translations/bufr/" + self.approach,
            file_dir=self.config_dir )
        self.bufr_flags         = gf.read_yaml( "codes/bufr/flags_" + self.approach,
            file_dir=self.config_dir )

        # remove unit translations (first 5 keys)
        self.bufr_translation_keys  = tuple(self.bufr_translation)[5:]
        
        # get special types of keys
        self.depth_keys, self.height_keys = set(), set()
        
        # keys where a duration is already defined and does not change
        self.fixed_duration_keys = set()
        
        match approach:
            
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
                            
                            if subkey[1] is not None:
                                self.fixed_duration_keys.add(subkey[1])

                self.bufr_translation = copy(self.bufr_translation_codes)

                self.obs_time_keys  = set(self.bufr_translation_keys) | set(self.time_keys) - {"cloudBase"}
                self.relevant_keys  = self.obs_time_keys | self.station_keys | self.typical_keys

                # all codes which contain timePeriod information for our synoptic purposes
                self.tp_codes       = {4023, 4024, 4025} # d, h, min
                self.tp_range       = range(4023, 4026)
                #self.tp_codes       = set(self.tp_range)

                # codes which alter height/depth of sensor
                self.height_depth_codes = self.height_codes | self.depth_codes
                
                # codes that contain replication factors
                self.repl_codes = {31000, 31001, 31002}
                #TODO maybe add 31011, 31012 (not used by DWD but maybe other providers do use them)
                
                # all codes which modify the following keys duration, height, depth and so on
                self.modifier_codes = self.tp_codes|self.height_depth_codes|self.repl_codes | {1023}
                #TODO maybe add 31011, 31012 (not used by DWD but maybe other providers do use them)

                self.datetime_codes = {
                    4001 : "year",  # XXXX
                    4002 : "month", # XX
                    4003 : "day",   # XX
                    4004 : "hour",  # XX
                    4005 : "minute" # XX
                }
                
                # codes for blockNumber and stationNumber
                self.station_codes  = {1001, 1002} # 1018
                
                if hasattr(self, "bufr_sequences"):
                    if type(self.bufr_sequences) == str:
                        self.bufr_sequences = gf.read_yaml( self.bufr_sequences,
                            file_dir=self.config_dir )
                    else: pass
                else:
                    self.bufr_sequences = gf.read_yaml( "codes/bufr/sequences",
                        file_dir=self.config_dir )

                self.sequence_range = range(min(self.bufr_sequences), max(self.bufr_sequences))
                self.scale_increase = 0

                self.scale_alter = {
                    202000 : 0, # reset scale
                    202129 : 1, # temporarily increase scale by 1 digit
                }

                # all code relevant for extraction of expanded descriptors
                self.relevant_codes = set( self.modifier_codes | set(self.bufr_translation) | set(self.scale_alter) ) - self.repl_codes
    
                self.size_alter = {
                    201000 : 0, # reset size
                    201132 : 4, # temporarily increase data size by 4 bits
                }
                
                self.scale_size_alter = set(self.scale_alter) | set(self.size_alter)

                self.repl_range     = range(101000, 131000) # range of repeated elements
                self.repl_seq_range = range(131000, 132000) # repeat next sequence (999 times)
                self.repl_info      = set( tuple(self.repl_codes) + tuple(self.repl_range) + tuple(self.repl_seq_range) )

            case "pd" | "pl" | "gt":
                
                for i in self.bufr_translation_keys:
                    if type(self.bufr_translation[i]) == dict:

                        try:    subkey = list(self.bufr_translation[i])[0]
                        except: continue

                        if self.bufr_translation[i][subkey][1] is not None:
                            self.fixed_duration_keys.add(i)

                        if type(subkey) == float:
                            if subkey   < 0: self.depth_keys.add(i)
                            elif subkey > 0: self.height_keys.add(i)

                    elif type(self.bufr_translation[i]) == list:
                        if self.bufr_translation[i][1] is not None:
                            self.fixed_duration_keys.add(i)

                self.bufr_obs_keys = set(self.bufr_translation_keys) - {"cloudBase"}

                self.ww             = "presentWeather"
                self.rr             = "totalPrecipitationOrTotalWaterEquivalent"
                self.wmo            = "WMO_station_id"
                self.dt             = "data_datetime"
                self.required_keys  = {self.wmo, self.dt}
                self.relevant_keys  = self.bufr_obs_keys | self.required_keys | self.modifier_keys | {self.obs_sequence} | self.replication_keys
                
                if approach in {"pl", "gt"}:
                    self.ignore_keys    = self.required_keys | self.replication_keys | {self.tp}
                    self.obs_list_keys  = self.relevant_keys - self.ignore_keys

        # we currently don't need these seperate key groups, outcommented for future use
        #self.height_keys, self.depth_keys   = nset(self.height_keys), set(self.depth_keys)
        # union of both will be used later
        self.height_depth_keys              = self.height_keys | self.depth_keys


    ### class lambda functions
    clear       = lambda self, keyname  : str( re.sub( r"#[0-9]+#", '', keyname ) )
    number      = lambda self, keyname  : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
    to_key      = lambda self, key, num : "#{num}#{key}"


    ### class functions
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

        # if duration is 0s, 1s or 1min: always leave it
        if key_db[1] in self.fixed_durations:
            duration = key_db[1]
        # if we already got a timePeriod from BUFR we dont need to translate it with the dict
        elif duration is None or key in self.fixed_duration_keys:
            duration = key_db[1]
        # None = null
        if duration is None: duration = "NULL"
        
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
        
        # if duration is 0s, 1s or 1min: always leave it
        if key_db[1] in self.fixed_durations:
            duration = key_db[1]
        # if we already got a timePeriod from BUFR we dont need to translate it with the dict
        elif duration is None or key in self.fixed_duration_keys:
            duration = key_db[1]
        # None = null
        if duration is None: duration = "NULL"

        return key_db[0], value, duration


    # version with units and scale (uses BUFR codes / expanded keys + values)
    def convert_keys_us( self, obs, dataset, shift_dt=False, convert_dt=False, verbose=False):
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
                    if shift_dt:
                        if datetime.minute in {0,30}:   datetime_db = datetime - td(minutes=10)
                        else:                           datetime_db = copy(datetime)
                    else: datetime_db = copy(datetime)

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
                            cor = int(val_obs)
                        elif code in self.tp_codes:
                            try:    duration    = self.bufr_translation[code][val_obs]
                            except: duration    = "" # TODO OR use continue to skip unknown duration?
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


    # extract values version uses same function
    convert_keys_ex = convert_keys_us

    
    # pdbufr / plbufr version
    def convert_keys_pd( self, obs, dataset, shift_dt=False, convert_dt=False, verbose=None ):
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
                        
                        if convert_dt:
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
                                elif key in {"heightOfBaseOfCloud", "cloudCoverTotal", "cloudAmount"}:
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

    # all of those approaches use the same function
    convert_keys_gt = convert_keys_pl = convert_keys_pd
