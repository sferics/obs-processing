import argparse
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
        from datetime import datetime as dt
        setattr(namespace, self.dest, dt.fromisoformat(value))

ToDt = ToDatetime

class ToTimedelta(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        from datetime import timedelta as td
        setattr(namespace, self.dest, td(*values))

ToTd = ToTimedelta

class ToDate(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        from date import date
        setattr(namespace, self.dest, date.fromisoformat(value))


split_set       = lambda values : set(values.split(","))
split_frozenset = lambda values : frozenset(values.split(","))
split_tuple     = lambda values : tuple(values.split(","))
split_list      = lambda values : values.split(",")
split_iter      = lambda values : iter(values.split(","))
split_range     = lambda values : range(values.split(","))


class ConfigClass:
    def __init__(self, script_name: str, pos: iter = [], flags: iter = [], info: str = "",
            config_file: str = "obs", parser_args: str = "parser_args", verbose: bool = False):
        
        args_dict   = gf.read_yaml(parser_args)
        args_pos    = args_dict["positional"]
        args_flags  = args_dict["flags"]
        self.psr    = argparse.ArgumentParser(description=info)
        
        for p in pos:
            arg_p = args_pos[p]
            self.psr.add_argument( p, nargs=arg_p[0], default=arg_p[1], help=arg_p[2], action=ToTuple )
        
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
        
        # parse the command line arguments
        self.args       = self.psr.parse_args()
        
        #TODO instead of setting the config file only -C should better define the whole config dir
        # if the config_file flag is present in parser_args.yml and the flag is turned on
        if hasattr(self.args, "config_file") and self.args.config_file:
            # overwrite config file name (default: obs[.yml])
            config_file = self.args.config_file
        
        self.config         = gf.read_yaml(config_file)
        self.script_name    = script_name
        
        # make all keys of config dict into class attributes for easier access
        for key, dic in self.config.items():
            setattr(self, key, dic)
        
        # take script name as argument & combine config_general+config_script (script has priority)
        self.script_raw = self.scripts[script_name]
        self.script     = self.general | self.script_raw
        
        # command line arguments overwrite settings in script config OR can even add new keys
        for key, val in self.args.__dict__.items():
            if val is not None: self.script[key] = val
