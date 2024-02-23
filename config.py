import argparse
import global_functions as gf

# https://stackoverflow.com/questions/52132076/argparse-action-or-type-for-comma-separated-list
class SplitArgs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))

class ToTuple(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, tuple(values))

split_set   = lambda values : frozenset(values.split(","))
split_tuple = lambda values : tuple(values.split(","))


class ConfigClass:
    def __init__(self, script_name: str, pos: iter, flags: iter, info: str, config: str = "obs",
            parser_args: str = "parser_args", verbose: bool = False):
        
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
        
        # if the config_file flag is present in parser_args.yml and the flag is turned on
        if hasattr(self.args, "config_file") and self.args.config_file:
            # overwrite config file name (default: obs[.yml])
            config = self.args.config_file
        
        config          = gf.read_yaml(config)
        self.config     = config
        # take script name as argument & combine config_general+config_script (script has priority)
        self.script_raw = config["scripts"][script_name]
        self.general    = config["general"]
        self.script     = self.general | self.script_raw
        self.classes    = { i: config[i] for i in config if i[0].isupper() }
        
        # classes will get there own attributes containing their dict; starting with lowercase
        for key, dic in self.classes.items():
            if verbose: print(key.lower(), dic)
            setattr(self, key.lower(), dic)

        self.clusters   = config["clusters"]
        self.sources    = config["sources"]

        # command line arguments overwrite settings in script config OR can even add new keys
        if verbose: print("ARGS")
        for key, val in self.args.__dict__.items():
            if verbose: print(key, val)
            if val is not None: self.script[key] = val
