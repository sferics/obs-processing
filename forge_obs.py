#!${CONDA_PREFIX}/bin/python
# CHAIN SCRIPT

# 1 reduce obs (only one for each [datetime,duration,element])
# 2 copy all elements from element table to forge databases
# in forge databases do:
# 3 audit_obs.py        -> each obs gets audited==True if succesful
# 4 derive_obs.py       -> RH+T=TD; cloud levels
# 5 aggregate_obs.py    -> aggregate over time periods (3,6,12,24h)
# 6 copy all obs from forge to dev or live database, depending on operation mode
# 7 clear forge datases
