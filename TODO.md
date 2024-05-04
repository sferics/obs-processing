- implement fallback elements in aggregate\_obs.py

- use dict.get(key) instead of always checking first whether key is in dict
- find out all units and conversion from metwatch.cfg and match units in amalthea/config/lookup.yml:metwatch and config/bufr\_translation_??.yml
- unit of HLC\_2m\_syn? really m/30?
- double-check and define all UNITS and store them in element\_table (add [unit, factor, offset] columns)

- outcomment all unneeded, experimental params like WDIRmax, VISpre
- fixed durations for params like CB, PRMSL, RCDC (extra column or define 1s, 1min as fixed?)

- reduce obs (highest scale -> highest priority source -> latest COR)

- implement -N flag (skip nulls, default off) --> explicitly save 'NULL'
- implement -w WARNING flag (define warning level or filter instead of just on/off switch)
- implement -p PROFILER flag (with fixed profiler or customizable - which would be harder)

- develop true multiprocessing for 'decode\_bufr.py' (using multiprocessing and shelves)

- bring add\_station\_from\_bufr back to working, code add\_station\_from\_source + special functions for sources

- delete all old / unneeded files

- rename source in files\_table to dataset?

- BUGFIXING
- COMMENTING
- DOCUMENTATION
- UNIT TESTING
- DEPLOY and INTEGRATE into Amalthea/main
