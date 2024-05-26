import argparse
from datetime import datetime as dt
from datetime import timedelta as td
from datetime import date
import global_functions as gf

# https://stackoverflow.com/questions/52132076/argparse-action-or-type-for-comma-separated-list
class SplitArgs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))

class ToSet(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, set(values))

class ToFrozenset(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, frozenset(values))

ToFset = ToFrozenset

class ToTuple(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, tuple(values))

class ToList(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, list(values))

class ToDict(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict(values))

class ToIter(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, iter(values))

class ToRange(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, range(values))

class ToSlice(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, slice(values))

class ToStr(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, str(value))

class ToBool(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, bool(value))

class ToInt(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, int(value))

class ToFloat(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, float(value))

class ToEval(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, eval(value))

class ToPath(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        from path import Path
        setattr(namespace, self.dest, Path(value))

class ToDatetime(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, dt.fromisoformat(value))

ToDt = ToDatetime

class ToTimedelta(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, td(*values))

ToTd = ToTimedelta

class ToDate(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, date.fromisoformat(value))


split_set       = lambda values : set(values.split(","))
split_frozenset = lambda values : frozenset(values.split(","))
split_fset      = split_frozenset
split_tuple     = lambda values : tuple(values.split(","))
split_list      = lambda values : values.split(",")
split_iter      = lambda values : iter(values.split(","))
split_range     = lambda values : range(values.split(","))
split_slice     = lambda values : slice(values.split(","))
split_datetime  = lambda values : dt(*values.split("-"))
split_dt        = split_datetime
split_timedelta = lambda values : td(*values.split(","))
split_td        = split_timedelta
split_date      = lambda values : date(*values.split("-"))


class ConfigClass:
    def __init__( self, script_name: str, pos: iter = [], flags: iter = [], info: str = "",
            config_dir: str = "config", sources: bool = False, clusters: bool = False ):
        """
        Parameter:
        ----------
        script_name : name of script to get config for
        pos : list of positional arguments
        flags : list of flags
        info : info message string
        config_dir : custom config directory path
        sources : read sources config file into self.sources
        clusters : read clusters config file into self.clusters

        Notes:
        ------
        the __init__ function creates the config class object 

        Return:
        -------
        class object
        """
        
        args_dict   = gf.read_yaml("parser_args", file_dir=config_dir)
        args_pos    = args_dict["positional"]
        args_flags  = args_dict["flags"]
        self.psr    = argparse.ArgumentParser(description=info)
        
        for p in pos:
            arg_p = args_pos[p]
            self.psr.add_argument( p, nargs=arg_p[0], default=arg_p[1], help=arg_p[2],
                    action=ToTuple )
        
        for f in flags:
            arg_f = args_flags[f]
            if arg_f[1]:
                self.psr.add_argument( f"-{f}", f"--{arg_f[0]}", choices=arg_f[1], nargs=arg_f[2],
                        action=arg_f[3], default=arg_f[4], type=eval(arg_f[5]), help=arg_f[6] )
            elif arg_f[3]:
                action = arg_f[3]
                #if action == "split_args": action = SplitArgs
                self.psr.add_argument( f"-{f}", f"--{arg_f[0]}", action=action,
                        default=arg_f[4], help=arg_f[6] )
            else:
                self.psr.add_argument( f"-{f}", f"--{arg_f[0]}", action=arg_f[3],
                        default=arg_f[4], type=eval(arg_f[5]), help=arg_f[6] )
        
        # parse the command line arguments using configparser module
        self.args           = self.psr.parse_args()
        
        # overwrite path of config directory (default: config) if -C/--config_dir flag is defined
        if hasattr(self.args, "config_dir") and self.args.config_dir:
            self.config_dir = self.args.config_dir
        else: # use the argument config_dir from class __init__
            self.config_dir = config_dir
        
        # read most important config files which we always need (scripts and general)
        self.scripts    = gf.read_yaml("scripts", file_dir=self.config_dir)
        general_config  = gf.read_yaml("general", file_dir=self.config_dir)
        
        # make all keys of general config dict into class attributes for easier access
        for section in general_config.keys():
            setattr(self, section, {})
            for key, val in general_config[section].items():
                getattr(self, section)[key] = val

        # sources and clusters config files will only be read if the respective arguments are set
        if sources:
            self.sources    = gf.read_yaml("sources", file_dir=self.config_dir)
        if clusters:
            self.clusters   = gf.read_yaml("clusters", file_dir=self.config_dir)
        
        # make script name a class attribute to make it accessible later on
        self.script_name = script_name
        
        # take script name as argument & combine config_general+config_script (script has priority)
        self.script_raw = self.scripts[script_name]
        self.script     = self.general | self.script_raw
        
        # command line arguments overwrite settings in script config OR can even add new keys
        #TODO dict comprehension?
        for key, val in self.args.__dict__.items():
            #self.script[key] = val
            if val is not None: self.script[key] = val
