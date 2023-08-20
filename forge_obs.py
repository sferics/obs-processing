#!/usr/bin/env python
# THIS IS A CHAIN SCRIPT

import os, sys

# 1 reduce_obs.py (only 1 row with max(file) per dataset [UNIQUE datetime,duration,element])
#   copy all remaining elements from raw to forge databases [datetime,duration,element,value]
# -in forge databases do:
# 2 audit_obs.py        -> check each obs, delete bad data like NaN, unknown value or out-of-range
#   OR instead(?): move bad data to seperate databases in "/[dev/oper]/bad" directory
# 3 derive_obs.py       -> compute derived elements like RH+T=TD; cloud levels; reduced pressure...
# 4 aggregate_obs.py    -> aggregate over time periods (1,3,6,12,24h) and create new elements
# 5 conclude_obs.py/finalize_obs.py
#   copy all relevant obs elements (main database element_table) from forge to dev or oper database,
#   depending on operation mode; if this action is complete, maybe do some last checks? afterwards:
#   clear all forge databases (they are just temporary and will be rebuilt in every chain cycle)

scripts = ("reduce", "audit", "derive", "aggregate", "conclude" )

args = ()

for script in scripts:
    try:    os.execv( "python", script, *args )
    except: continue
