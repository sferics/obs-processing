import argparse
from global_functions import read_yaml

class ConfigClass:
    def __init__(self, config={}, pos={}, flags={}, info="", parser_args="parser_args", verbose=False):
        
        self.config = config
        self.psr    = argparse.ArgumentParser(description=info)
        args_dict   = read_yaml(parser_args)
        args_pos    = args_dict["positional"]
        args_flags  = args_dict["flags"]
        
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

        self.args = self.psr.parse_args()

        # each setting in config will become a class attribute
        if verbose: print("CONFIG")
        for c in config:
            if verbose: print(c, config[c])
            setattr(self, c, config[c])
        
        # command line arguments overwrite settings in config
        if verbose: print("ARGS")
        for attr, val in self.args.__dict__.items():
            if verbose: print(attr, val)
            setattr(self, attr, val)
