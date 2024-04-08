#!/home/dev/bin/miniconda3/envs/test39/bin/python

from pathlib import Path
from datetime import datetime as dt, timezone as tz
import numpy as np
import os
from psutil import pid_exists, Process
import subprocess, sys
from copy import copy
import sqlite3
import global_variables as gv


### lambda functions
# https://stackoverflow.com/questions/74170251/check-whether-a-list-contains-a-value-and-if-yes-return-it
value_in_list       = lambda lst, val : val if val in lst else None
to_datetime         = lambda DT : dt(DT["year"], DT["month"], DT["day"], DT["hour"], DT["minute"])
to_datetime_hour    = lambda DT : dt(DT["year"], DT["month"], DT["day"], DT["hour"])


def import_from(module_or_file, obj, globs, locs):
    """
    Notes:
    ------
    import object(s) from module by their string names
    """
    if type(obj) == str:
        namespace = __import__(module_or_file, globs, locs, [obj], 0)
        return getattr(namespace, obj)
    elif hasattr(obj, "__iter__"):
        namespace = __import__(module_or_file, globs, locs, obj, 0)
        return ( getattr(namespace, i) for i in obj )
    else: raise ValueError("obj has to be str or iterable")

def values_in_list(lst, vals):
    """
    """
    for val in vals:
        if val in lst: return val
    return None

#TODO maybe we need a LoggerClass??? what could be the benefits???
def get_logger(script_name, log_level="NOTSET", log_path="log", mode="w", formatter=""):
    """
    Parameter:
    ----------

    Notes:
    ------
    inspired by: https://stackoverflow.com/a/69693313

    Return:
    -------

    """
    from global_variables import log_levels
    assert(log_level in log_levels)
    import logging, logging.handlers

    if not os.path.exists(log_path): os.makedirs(log_path)
    
    logger = logging.getLogger(script_name)
    logger.setLevel( getattr(logging, log_level) )
    
    if formatter:
        #formatter = logging.Formatter('%(asctime)s:%(levelname)s : %(name)s : %(message)s')
        formatter = logging.Formatter(formatter)
        file_handler.setFormatter(formatter)
    
    file_handler = logging.FileHandler(f"{log_path}/{script_name}.log", mode=mode)
    if logger.hasHandlers(): logger.handlers.clear()
    logger.addHandler(file_handler)
    
    return logger


def get_script_name(FILE, realpath=True):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    Name of the current script. if realpath is True, symlinks are resolved

    """ 
    if realpath:
        path = os.path.realpath(FILE)
        return path.split("/")[-1]
    else: return os.path.basename(FILE)


def print_time_taken(start_time, stop_time, precision=3):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    None
    """
    time_taken      = stop_time - start_time
    time_taken_deci = int(time_taken.microseconds / (1e6 / 10**precision) )
    print(f"{time_taken.seconds}.{time_taken_deci} s")


def chunks(l, n):
    """Yield n number of striped chunks from l."""
    for i in range(0, n):
        yield l[i::n]


def merge_list_of_dicts( list_of_dicts, add_keys=True ):
    # flatten a list of dict to a single dict, inspired by:
    #https://stackoverflow.com/questions/3494906/how-do-i-merge-a-list-of-dicts-into-a-single-dict#comment129437582_60139123
    if add_keys:
        from operator import __ior__
        from functools import reduce
        return reduce( __ior__, list_of_dicts, {} )
    else:
        out_dict = list_of_dicts[0]
        for i in range(1, len(list_of_dicts)):
            for key in list_of_dicts[i]:
                if key in list_of_dicts[i-1]: out_dict[key] = list_of_dicts[i][key]
        return out_dict


# inspired by: https://stackoverflow.com/questions/17694261/how-can-i-perform-set-operations-on-python-dictionaries
def dict_ops(d1, d2, setop, keep_vals=1):
    """
    Apply set operation `setop` to dictionaries d1 and d2

    Notes:
    ------
    In cases where values are present in both d1 and d2, the value from
    d1 will be used - unless keep_vals is set == 2
    """
    if keep_vals == 2:
        d1, d2, = d2, d1
    return { k : d1.get( k, k in d1 or d2[k] ) for k in setop( set(d1), set(d2) ) }


def fname():
    """
    Return:
    -------
    get the frame object of the frame or function
    """
    frame = inspect.currentframe()
    return frame.f_code.co_name


def print_trace(e = BaseException):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    None
    """ 
    import traceback
    print(''.join(traceback.TracebackException.from_exception(e).format()))


def create_dir( path, exist_ok=True, parents=True ):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    None
    """ 
    Path( path ).mkdir( exist_ok=exist_ok, parents=parents )


def read_file(file_name):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    content of file as str
    """ 
    return Path( file_name ).read_text()


def read_yaml(file_name="obs", directory="config", ext="yml", typ="safe", pure=True,
        duplicate_keys=False, values={}, autoformat=False):
    """
    Parameter:
    ----------
    file_name : name of the yaml file we want to parse
    directory : dir (or path without file_name) of the file
    typ : input for ruamel.yaml.YAML() can be [ safe, rt, jinja2 ]
    pure : bool, see https://stackoverflow.com/questions/51316491/ruamel-yaml-clarification-on-typ-and-pure-true
    duplicate_keys : allow duplicate keys (which violate the yaml specification, so be careful!)
    values : dict of values we want to replace in the yaml, syntax: {{value}}
    autoformat : bool, if True does not require !!format tag and overwrites standard yaml inpretation of {}

    Notes:
    ------
    loads a yaml file and returns a dictionary of its original json-like content and structure

    Return:
    -------
    content of yaml file as dict
    """
    import ruamel.yaml as yaml
    import warnings
    warnings.simplefilter('ignore', yaml.error.UnsafeLoaderWarning)
    
    yaml.allow_duplicate_keys = duplicate_keys
    
    loader = yaml.YAML(typ=typ, pure=pure)

    match typ:
        case "rt" | "safe": pass # use rountrip or safe loader; nothing else to do here
        case "pypyr":
            # use pypyr features (see documentation https://pypyr.io/docs/context-parsers/yamlfile)
            from pypyr.context import Context
            from pypyr.dsl import Jsonify, PyString, SicString
            
            for representer in (Jsonify, PyString, SicString): loader.register_class(representer)

            # Context is a dict data structure, so can just use a dict representer
            represent_dict = loader.representer.represent_dict
            loader.Representer.add_representer( Context, represent_dict )
        case "jinja2":
            # add this nice constructor if jinja is used: https://stackoverflow.com/a/62979185
            from jinja2 import Template, Undefined
            from ruamel.yaml.resolver import BaseResolver

            class NullUndefined(Undefined):
               def __getattr__(self, key):
                 return ''

            def resolve_in_dict( loader: loader, node: yaml.Node ):
                assert isinstance( node, yaml.MappingNode )
                values = loader.construct_mapping( node, deep=True )
                for key, value in values.items():
                    if isinstance(value, str):
                        t = Template( value, undefined=NullUndefined )
                        values[key] = t.render(values)
                return values

            yaml.add_constructor( BaseResolver.DEFAULT_MAPPING_TAG, resolve_in_dict )
        case _: raise NotImplementedError(f"Unknown typ: '{typ}'")
    

    # the following code was borrowed from here: https://stackoverflow.com/a/65516240
    def flatten_sequence(sequence: yaml.Node):
        """
        Parameter:
        ----------

        Notes:
        ------
        Flatten a nested sequence to a list of integers. A nested structure is always a SequenceNode

        Return:
        -------
        generator which yields list of integers
        """
        if isinstance(sequence, yaml.ScalarNode):
            yield sequence.value
            return
        if not isinstance(sequence, yaml.SequenceNode):
            raise TypeError(f"'!flatten' can only flatten sequence nodes, not {sequence}")
        for el in sequence.value:
            if isinstance(el, yaml.SequenceNode):
                yield from flatten_sequence(el)
            elif isinstance(el, yaml.ScalarNode):
                yield int(el.value) # TODO make this type-independent (str, float)
            else:
                raise TypeError(f"'!flatten' can only take scalar nodes, not {el}")

    def construct_flat_list(loader: loader, node: yaml.Node):
        """
        Parameter:
        ----------
        loader: Unused, but necessary to pass to `yaml.add_constructor`
        node: The passed node to flatten

        Notes:
        ------
        Make a flat list, should be used with '!flatten'

        Return:
        -------
        list of integers
        """
        return list(flatten_sequence(node))
    
    #TODO can only flatten lists containing ints so far; see above 'flatten_sequence'
    yaml.add_constructor("!flatten", construct_flat_list)
    # borrowed end
        
    def construct_bool(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.ScalarNode):
            return bool(int(node.value))
        else: raise TypeError("node.value needs to be a scalar")

    def construct_eval(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.ScalarNode):
            if type(node.value) == str:
                return eval(node.value)
            else: raise TypeError("node.value needs to be a scalar (of type str)")
        else: raise TypeError("node.value needs to be a scalar")

    def yield_sequence(sequence, call = None):
        for el in sequence.value:
            if call:    yield call(el.value)
            else:       yield el.value

    def construct_frozenset(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.SequenceNode):
            return frozenset(yield_sequence(node))
        else: raise TypeError("node.value needs to be a sequence")

    def construct_set(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.SequenceNode):
            return set(yield_sequence(node))
        else: raise TypeError("node.value needs to be a sequence")

    def construct_tuple(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.SequenceNode):
            return tuple(yield_sequence(node))
        else: raise TypeError("node.value needs to be a sequence")

    def construct_list(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.SequenceNode):
            return list(yield_sequence(node))
        else: raise TypeError("node.value needs to be a sequence")

    def construct_iter(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.SequenceNode):
            yield yield_sequence(node)
        else: raise TypeError("node.value needs to be a sequence")
    
    def construct_range(loader: loader, node: yaml.Node):
        if isinstance(node, yaml.SequenceNode):
            elements = tuple(yield_sequence(node, call=int))
            if 1 <= len(elements) <= 3:
                return range(*elements)
            else: raise TypeError("node.value needs to be a sequence (of length 1-3)")
        else: raise TypeError("node.value needs to be a sequence")

    for tag in ("bool", "eval", "frozenset", "set", "tuple", "list", "iter", "range"):
        construct_function = locals()[f"construct_{tag}"]
        yaml.add_constructor(u'tag:yaml.org,2002:'+tag, construct_function)

    if values:
        # another "borrowing" - I promise to give it back ASAP! https://stackoverflow.com/a/53043084
        def construct_format(loader: loader, node: yaml.Node):
            return loader.construct_scalar(node).format(**values)
        
        if autoformat:  fmt = u'tag:yaml.org,2002:str'
        else:           fmt = "!format"
        yaml.add_constructor(fmt, construct_format)
        # borrowed end


    # this is the important part; load the file read-only with the chosen method and return it as dict
    with open( directory + "/" + file_name + "." + ext, "rt" ) as file_handle:
        
        # if typ == pypyr: use the features from pypyr package and return dictitems a dictionary
        if typ == "pypyr":
            res = loader.load(file_handle)
            yml = yaml.YAML()
            return dict( yml.load(yaml.dump(res)) )["dictitems"]

        # else just use the ruamel.yaml loader
        else: return yaml.load(file_handle)


def get_file_path( FILE, string=True ):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    file path as str
    """ 
    PATH = Path( FILE ).resolve().parent
    if string:
        return str( PATH )
    return PATH


def get_file_date( file_path, datetime=True ):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    file date as datetime object (default) or str
    """ 
    date = Path(file_path).stat().st_mtime
    if datetime:
        return ts2dt( date )
    return date


def get_input_files_dict( config_database, input_files=[], source="extra",
        config_source: dict = {}, PID=None, redo=False, log=None, verbose=False ):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    """
    from database import DatabaseClass
    db = DatabaseClass(config=config_database)

    files_dict      = {}
    status_locked   = "locked"

    if PID: status_locked += str(PID)
    
    try:    redo        = config_source["redo"]
    except: pass

    if input_files:
        
        for file_path in input_files:
            
            file_name   = file_path.split("/")[-1]
            file_date   = get_file_date(file_path)
            file_dir    = "/".join(file_path.split("/")[:-1])
            ID          = db.get_file_id(file_name, file_dir)
            
            if ID:
                if not redo:
                    if db.get_file_status(ID) in gv.skip_status:
                        continue
                    else: db.set_file_status(ID, status_locked)
            else:
                ID = db.register_file(file_name, file_path, source, status_locked, file_date, 0, 0)
                if not ID:
                    print(f"REGISTERING FILE '{file_path}' FAILED!")
                    if log: log.error(f"REGISTERING FILE '{file_path}' FAILED!")
                    continue

            files_dict[ID] = { "name":file_name, "dir":file_dir+"/", "date":file_date }
        
        db.close(commit=True)
    
    elif source and config_source:
        source_dir  = config_source["dir"] + "/"
        try:    sort_files  = config_source["sort_files"]
        except: sort_files  = False
        try:
            if callable(config_source["sort_method"]):
                sort_method = config_source["sort_method"]
            else: sort_method = sorted
        except: sort_method = sorted
        try:    max_files   = config_source["max_files"]
        except: max_files   = None
        try:    glob        = config_source["glob"]
        except: glob        = "*"
        try:    ext         = config_source["ext"]
        except: ext         = "*"
        try:    restart     = config_source["restart"]
        except: restart     = False

        glob_ext = f"{glob}.{ext}"

        if restart:
            files_to_parse = db.get_files_with_status( f"locked_{restart}", source )
        else:
            from glob import glob
            files_in_dir   = { os.path.basename(i) for i in glob( source_dir + glob_ext ) }

            if redo:    skip_files  = db.get_files_with_status( r"locked_%", source )
            else:       skip_files  = db.get_files_with_status( gv.skip_status, source )

            files_to_parse = list( files_in_dir - skip_files )

            #TODO special sort functions for CCA, RRA and stuff in case we dont have sequence key
            #TODO implement order by datetime (via sort_method callable)
            if sort_files: files_to_parse = sort_method(files_to_parse)
            if max_files:  files_to_parse = files_to_parse[:max_files]

            if verbose:
                print("#FILES in DIR:  ",   len(files_in_dir))
                print("#FILES to skip: ",   len(skip_files))

        if verbose: print("#FILES to parse:",   len(files_to_parse))

        for file_name in files_to_parse:

            file_path = get_file_path( source_dir + file_name )
            file_date = get_file_date( file_path )

            ID = db.get_file_id(file_name, file_path)
            if not ID:
                if PID: status_locked = f"locked_{PID}"
                else:   status_locked = "locked"
                ID = db.register_file(file_name, file_path, source, status_locked, file_date, verbose=verbose)
                if not ID:
                    if log: log.error(f"REGISTERING FILE '{file_path}' FAILED!")
                    continue

            files_dict[ID] = { "name":file_name, "dir":source_dir, "date":file_date }

        db.close(commit=True)

        #TODO if multiprocessing: split files_to_parse by # of processes (e.g. 8) and parse files simultaneously
        #see https://superfastpython.com/restart-a-process-in-python/

    else: raise TypeError("Either source+config_source or input_files args have to be provided!")

    return files_dict


def dt2str( datetime, fmt ):
    #datetime -> string
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    datetime_str = datetime.strftime( fmt )
    return datetime_str


def dt2ts( datetime, min_time = False, tzinfo=tz.utc ):
    """
    Parameter:
    ----------

    Notes:
    ------
    convert today's datetime object to timestamp

    Return:
    -------

    """
    if min_time: dtts = dt.combine( datetime, dt.min.time() )
    else: dtts = datetime
    return int( dtts.replace( tzinfo = tz.utc ).timestamp() )


def str2dt( string, fmt, tzinfo=tz.utc ):
    """
    Parameter:
    ----------

    Notes:
    ------
    convert string to datetime object

    Return:
    -------

    """
    datetime = dt.strptime(string, fmt).replace( tzinfo=tzinfo )
    return datetime


def str2ts( string, fmt, min_time = False, tzinfo=tz.utc ):
    #string -> timestamp
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    datetime = str2dt( string, fmt )
    return dt2ts( datetime, min_time = min_time, tzinfo=tzinfo )


ts2dt = lambda ts : dt.fromtimestamp( ts )


def mins2hours( mins ):
    """
    Parameter:
    ----------
    mins : str of form "*min"

    Return:
    -------
    str in hours if the input str was in hours, else mins
    """
    mins_int = int(mins[:-3])
    if mins_int >= 60:
        return str(int( mins_int / 60 )) + "h"
    else: return mins


def hours2mins( hours ):
    """
    Parameter:
    ----------
    hours : str of form "*h"

    Return:
    -------
    str in mins if the input str was in mins, else hours
    """
    hours_float = float(hours[:-1])
    if hours_float < 1:
        return str(int( hours_float * 60)) + "min"
    else: return hours


dt_str = lambda integer : str(integer).rjust(2, "0")

def already_running( pid_file = "pid.txt" ):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    True if pid file exists, False if not
    """ 
    #https://stackoverflow.com/a/73363976
    if os.path.exists( pid_file ):
        return True
    with open( pid_file, 'w' ) as f:
        f.write( str(os.getpid()) )
    return False


def already_running2():
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------
    True if script is already running, False if not
    """ 
    cmd = [f'pgrep -f .*python.*{sys.argv[0]}']
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
    my_pid, err = process.communicate()

    if len(my_pid.splitlines()) >0 :
       return True
    return False


# meteorological functions
def rh2dpt( rh, tmp, perc=True, C_in=True, C_out=True ):
    #TODO
    """
    Parameter:
    ----------
    rh : relative humidity in percent or [0,1]
    tmp : temperature in K or C
    perc : input of rh in percent if True; else [0,1]
    C_in : input in Celsius if True; else Kelvin
    C_out : output in Celsius 

    Notes:
    ------
    takes relative humidity and temperature as input and calculates the dewpoint temperature

    Return:
    -------
    dewpoint in K or C
    """
    # convert Kelvin to Celsius if needed
    if not C_in: tmp += gv.K

    #https://www.omnicalculator.com/physics/relative-humidity
    if perc: rh /= 100
    
    ln_rh = np.log( rh )
    bt_lt = ln_rh + (gv.beta * tmp) / (gv.lamb + tmp)
    
    dpt = (gv.lamb * bt_lt) / (gv.beta - bt_lt)
    
    if not C_out: dpt += gv.K
    return dpt


def dpt2rh(dpt, tmp, perc=True, C_in=True):
    """
    Parameter:
    ----------
    dpt : dewpoint temperature in K or C
    tmp : temperature in K or C
    C_in : input in Celsius (if True: Celsius; else: Kelvin)
    perc : output in percent instead of [0,1] 

    Notes:
    ------
    takes dewpoint and temperature as input and calculates the relative humidity

    Return:
    -------
    relative humidity in percent or [0,1]
    """
    # convert Kelvin to Celsius if needed
    if not C_in:
        dpt += gv.K
        tmp += gv.K
    
    exp_dpt = np.exp( (gv.beta*dpt) / (gv.lamb+dpt) )
    exp_tmp = np.exp( (gv.beta*tmp) / (gv.lamb+tmp) )
    
    rh = exp_dpt / exp_tmp
    
    if perc: rh *= 100
    
    return rh


def qfe( ppp, h, tmp, C_in=True ):
    """
    C_in : input in Celsius (if True: Celsius; else: Kelvin)
    """
    # convert Celsius to Kelvin if needed
    if C_in: tmp += gv.K

    return p * ( 1 + (gv.g * h) / (gv.R * tmp) )


def svp(tmp):
    """
    Parameter:
    ----------
    tmp : temperature in Celsius
    """
    return gv.C1 * np.exp( gv.C2 * tmp / (gv.C3 + tmp) )


#DWD standard pressure reduction method
def qff_dwd( ppp, h, tmp, rh, C_in=True ):
    """
    Parameter:
    ----------
    ppp : pressure in hPa
    h : height of barometer
    tmp : temperature in Celsius
    rh : relative humidity (in percent)
    C_in : input in Celsius (if True: Celsius; else: Kelvin)
    """
    # convert betwen Celsius and Kelvin
    if C_in:
        tmp_C = copy(tmp)
        tmp_K = tmp + gv.K
    else:
        tmp_C = tmp - gv.K 
        tmp_K = copy(tmp) 
    
    VP  = svp(tmp_C) * (rh / 100)
    EXP = np.exp( gv.g / (gv.R * h) / (tmp_K + VP * gv.Ch + gv.a * h / 2) )
    return ppp * EXP


#source for this pressure reductions: https://www.metpod.co.uk/calculators/pressure/
def qff_smhi( ppp, h, tmp, lat, C_in=True ):
    #TODO lat to phi, doctring
    """
    Parameter:
    ----------
    ppp : pressure in hPa
    h : height of barometer
    tmp : temperature in Celsius or Kelvin
    lat : latitude (float between 0 and 1)
    C_in : input in Celsius (if True: Celsius; else: Kelvin)

    Notes:
    ------

    Return:
    -------

    """
    # convert Kelvin to Celsius if needed
    if not C_in: tmp -= gv.K

    if tmp < -7: #°C!
        tmp = 0.5*t + 275 #K!
    elif -7 <= tmp <= 2:
        tmp = 0.535*tmp + 275.6
    else: # tmp > 2
        tmp = 1.07*tmp + 274.5
    a = 0.034163; b = 0.0026473

    return ppp * np.exp( ( h*a * (1-b*np.cos(lat)) ) / tmp ) 


def qnh( ppp, h, tmp, C_in=True ):
    """
    Parameter:
    ----------
    ppp : pressure in hPa
    h : height of barometer
    tmp : temperature in Kelvin or Celsius

    Notes:
    ------

    Return:
    -------

    """
    # convert Kelvin to Celsius if needed
    if not C_in: tmp -= gv.K

    a = 18429.1; b = 67.53; c = 0.003
    return h / (a + b*tmp + c*h)
