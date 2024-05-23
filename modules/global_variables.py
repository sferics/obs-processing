# global variables

import datetime
import numpy as np

# default logger level
logger_level='WARNING' # options are: DEBUG, INFO, WARNING, ERROR, CRITICAL, NOTSET

# all available log levels
log_levels = { "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET" }

# generic variables that signify empty or non-numeric values
null_vars = { np.nan, np.inf, np.NINF, "", None, }

# all types / vars that return False ( if X in not_types: => True )
not_types = ( None, 0, False, "", {}, [], (), set(), frozenset() )

# all array-like python types; you can check "X in array_types"
array_types = frozenset( { list, tuple, set, frozenset } )

# file statuses to skip usually
skip_status = {"locked_.", "error", "empty", "parsed"}

# meteorological constants
K   = 273.15 # conversion between Celsius and Kelvin
g   = 9.80665 # m/s**2
R   = 8.31446261815324
a   = 0.0065 # K/m
Ch  = 0.12 # K/hPa

C1  = 6.11213
C2  = 17.5043
C3  = 241.2

beta    = 17.625
lamb    = 243.04 #-30.11 
