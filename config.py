import argparse
import global_functions as gf

class ConfigClass:
    def __init__(self, script_name, config="config", pos={}, flags={}, info="", parser_args="parser_args", verbose=False):
        
        args_dict   = gf.read_yaml(parser_args)
        args_pos    = args_dict["positional"]
        args_flags  = args_dict["flags"]
        self.psr    = argparse.ArgumentParser(description=info)

        for p in pos:
            arg_p = args_pos[p]
            self.psr.add_argument( p, nargs=arg_p[0], default=arg_p[1], help=arg_p[2] )

        for f in flags:
            arg_f = args_flags[f]
            if arg_f[1]:
                self.psr.add_argument( f"-{f}", f"--{arg_f[0]}", choices=arg_f[1],
                        action=arg_f[2], default=arg_f[3], help=arg_f[4] )
            else:
                self.psr.add_argument( f"-{f}", f"--{arg_f[0]}", action=arg_f[2],
                        default=arg_f[3], help=arg_f[4] )

        self.args   = self.psr.parse_args()

        self.cli    = []

        if verbose: print("PARSED ARGS")
        for attr, val in self.args.__dict__.items():
            if verbose: print(attr, val)
            #TODO get shorthand flag by name and add command line args to self.cli

        if self.args.config: config = self.args.config

        config          = gf.read_yaml( config )
        self.config     = config
        # take script name as argument & combine config_general+config_script (script has priority)
        self.script_old = config["scripts"][script_name]
        self.general    = config["general"]
        self.script     = self.general | self.script_old
        self.classes    = {i:config[i] for i in config if i[0].isupper()}
        
        # classes will get there own attributes containing their dict; starting with lowercase
        for key, dic in self.classes.items():
            if verbose: print(key.lower(), dic)
            setattr(self, key.lower(), dic)

        #self.database   = config["Database"]
        #self.bufr       = config["Bufr"]
        #self.obs        = config["Obs"]
        
        self.clusters   = config["clusters"]
        self.sources    = config["sources"]

        # each setting in config will become a class attribute
        """
        if verbose: print("CONFIG")
        for c in config:
            if verbose: print(c, config[c])
            setattr(self, c, config[c])
        
        # command line arguments overwrite settings in script config
        if verbose: print("ARGS")
        for attr, val in self.args.__dict__.items():
            if verbose: print(attr, val)
            setattr(self.script, attr, val)
        """

        # command line arguments overwrite settings in script config
        if verbose: print("ARGS")
        for key, val in self.args.__dict__.items():
            if verbose: print(key, val)
            self.script[key] = val
