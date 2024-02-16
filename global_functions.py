#!/home/dev/bin/miniconda3/envs/test39/bin/python

from pathlib import Path
from datetime import datetime as dt, timezone as tz
import numpy as np
import os
from psutil import pid_exists, Process
import subprocess, sys
from copy import copy
import sqlite3


### lambda functions
to_datetime         = lambda DT : dt(DT["year"], DT["month"], DT["day"], DT["hour"], DT["minute"])
to_datetime_hour    = lambda DT : dt(DT["year"], DT["month"], DT["day"], DT["hour"])

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


#TODO add update=bool flag (ON CONFLICT DO UPDATE clause on/off)
def obs_to_station_databases( obs_db, source, output_path, max_retries=100, timeout=5, verbose=0 ):
    #TODO
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """ 
    from database import DatabaseClass
    # insert values or update value if we have a newer cor, then set parsed = 0 as well
    sql = ( f"INSERT INTO obs (dataset,file,datetime,duration,element,value,cor) VALUES " 
        f"('{source}',?,?,?,?,?,?) ON CONFLICT DO UPDATE SET value = excluded.value, reduced = 0, "
        f"file = excluded.file WHERE excluded.cor > obs.cor" )

    for loc in obs_db:
        created = create_station_tables(loc, output_path, "raw", max_retries, 1, 1, verbose=verbose)
        if not created: continue
       
        retries = copy(max_retries)

        while retries > 0:
            try:
                db_loc = DatabaseClass( f"{output_path}/raw/{loc[0]}/{loc}.db", timeout=timeout)
                db_loc.exemany( sql, obs_db[loc] )
            except sqlite3.Error as e:
                print(e, retries)
                retries -= 1
                if verbose: print(f"Retrying to insert data", retries, "times")
                continue
            else:
                if verbose:
                    print(loc)
                    loc = list(obs_db[loc])
                    for i in range(len(loc)):
                        if loc[i][5]:   cor = "CC" + chr(64+loc[i][5])
                        else:           cor = ""
                        print(f"{loc[i][1]} {loc[i][2]:<6} {loc[i][3]:<20} {loc[i][4]:<21} {cor}")
                    print()
                break

        db_loc.close(commit=True)


def create_station_tables( location, output_path, typ, max_retries=100, commit=True, traceback=True, verbose=None ):
    """
    Parameter:
    ----------
    location : station location, usually WMO ID
    output_path : where the station databases are saved
    commit : commit to database afterwards
    verbose : print exceptions that are being raised

    Notes:
    ------
    creates the obs,model and stats tables for a new station

    Return:
    -------
    True if succesful, False if not, None if tables already exists and completely setup (ready == 1)
    """
    from database import DatabaseClass
    station_path = f'{output_path}/{typ}/{location[0]}'
    create_dir( station_path )
    db_path = f'{station_path}/{location}.db'

    ready = False
    
    retries = copy(max_retries)

    while retries > 0:
        try:
            db = DatabaseClass( db_path )
            # get number of tables in attached DB
            db.exe(f"SELECT count(*) FROM sqlite_master WHERE type='table'")
            n_tables = db.fetch1()
        except sqlite3.Error as e:
            print(e, retries)
            retries -= 1
            continue
        else:
            ready = ( n_tables == 3 ) # 3 is hardcoded for performance reasons, remember to change!
            break

    if retries == 0: return False

    if ready:
        db.close()
        return True
    else:
        if verbose: print("Creating table and adding columns...")

        # read structure file station_tables.yaml into a dict
        tables = read_yaml( f"station_tables_{typ}" )

        for table in tables:
            retries = copy(max_retries)
            while retries > 0:
                try:
                    created = db.create_table( table, tables[table], verbose=verbose )
                except sqlite.Error as e:
                    print(e, retries)
                    retries -= 1
                    continue
                else:
                    if not created: retries -= 1; continue
                    else: break
    
            if retries == 0: return False

    db.close(commit=commit)
    return True


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


def read_yaml(file_name="config", directory="yaml", ext="yaml", typ="safe", pure=True, duplicate_keys=False, values={}, autoformat=False):
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

    if typ == "rt": # use roundtrip and features from pypyr (https://pypyr.io/docs/context-parsers/yamlfile/)
        from pypyr.context import Context
        from pypyr.dsl import Jsonify, PyString, SicString
        
        for representer in (Jsonify, PyString, SicString): loader.register_class(representer)

        # Context is a dict data structure, so can just use a dict representer
        represent_dict = loader.representer.represent_dict
        loader.Representer.add_representer( Context, represent_dict )

    elif typ == "jinja2":
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

        """
        return list(flatten_sequence(node))

    yaml.add_constructor("!flatten", construct_flat_list)
    # borrowed end


    if values:
        # another "borrowing" - I promise to give it back ASAP! https://stackoverflow.com/a/53043084
        def construct_format(loader: loader, node: yaml.Node):
            return loader.construct_scalar(node).format(**values)
        
        if autoformat:  fmt = u'tag:yaml.org,2002:str'
        else:           fmt = "!format"
        yaml.add_constructor(fmt, construct_format)
        # borrowed end


    # this is the important part; load the file read-only with the chosen method and return it as dict
    with open( directory + "/" + file_name + "." + ext, "rt" ) as f:
         
        if typ == "rt":
            res = loader.load(f)
            yml = yaml.YAML()
            return dict( yml.load(yaml.dump(res)) )["dictitems"]

        # else just use the ruamel.yaml loader
        else: return yaml.load(f)


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
def rh2dp( rh, T ):
    #TODO
    """
    Parameter:
    ----------
    rh : relative humidity in %
    T : temperature in K

    Notes:
    ------
    takes relative humidity and temperature as input and calculates the dewpoint temperature

    Return:
    -------
    dewpoint in K
    """ 
    #https://www.omnicalculator.com/physics/relative-humidity
    beta = 17.625
    lamb = -30.11

    ln_rh_100 = np.log( rh / 100 )
    bT_lT = (beta * T) + (lamb + T)

    return (lamb * bT_lT) / (beta - bT_lT)


#source for these pressure reductions: https://www.metpod.co.uk/calculators/pressure/
def qfe( ppp, h, t, lat ):
    #TODO lat to phi, doctring
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    if t < -7: #°C!
        t = 0.5*t + 275 #K!
    elif -7 <= t <= 2:
        t = 0.535*t + 275.6
    else: # t > 2
        t = 1.07*t + 274.5
    a = 0.034163; b = 0.0026473

    return ppp * np.exp( ( h*a * (1-b*np.cos(lat)) ) / t ) 


def qnh( ppp, h, t ):
    #TODO docstring
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    a = 18429.1; b = 67.53; c = 0.003
    return h / (a + b*t + c*h)
