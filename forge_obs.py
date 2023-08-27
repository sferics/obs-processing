#!/usr/bin/env python
# THIS IS A CHAIN SCRIPT

import os, sys
import subprocess

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

# delete_duplicate needs to be removed as soon as the bug which cause duplicates is fixed!!!
scripts = ("reduce", "audit", "derive", "aggregate", "delete_duplicate", "conclude" )

#TODO
args = ()

# https://stackoverflow.com/questions/8953119/waiting-for-external-launched-process-finish

for script in scripts:
    try:    os.execv( "python", script+"_obs.py", *args )
    except: continue
