# global variables

import datetime
import numpy as np

# date related global variables
start_date = datetime.datetime(2021,1,1)
end_date   = datetime.datetime(2023,1,1)

# release
release='2306'

# seasons
season_threshold=300
seasons = [('01-12', '03-01'), # added a day to avoid the variable ending of february
           ('02-03', '05-31'),
           ('06-01', '08-31'),
           ('09-01', '11-30')]

# logger level
logger_level='WARNING' # options are: DEBUG, INFO, WARNING, ERROR, CRITICAL, NOTSET

# all available log levels
log_levels = { "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET" }

# regorder_rules
# {iteration_phase : accepted_predictor_groups}
regorder={1:(1,2,3),
          2:(2,5),
          3:(1,2,3,4,5)}


# generic variables that signify empty or non-numeric values
null_vars = { np.nan, np.inf, np.NINF, "", None, }

# all types / vars that return False ( if X in not_types: => True )
not_types = ( None, 0, False, "", {}, [], (), set(), frozenset() )

# all array-like python types; you can check "X in array_types"
array_types = { list, tuple, set, frozenset }

# for BUFR obs decoding scrips
replication   = "delayedDescriptorReplicationFactor"
sensor_height = "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform"
sensor_depth  = "depthBelowLandSurface"
vertical_sigf = "verticalSignificanceSurfaceObservations"
obs_sequence  = "observationSequenceNumber"
modifier_keys = { sensor_height, sensor_depth, vertical_sigf }
