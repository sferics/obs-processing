- outcomment all unneeded, experimental params lik WDIRmax
- fixed durations for params like CB, PRMSL, RCDC (extra column or define 1s, 1min as fixed?)
- reduce obs (highest scale -> highest priority source -> latest COR)
- implement -N flag (skip nulls, default off) --> explicitly save 'null'
- implement -w warning flag (define warning level or filter instead of just on/off switch)
- check / define all UNITS and store them in some table / yaml file

- develop true multiprocessing for 'decode\_bufr.py' (using multiprocessing and shelves)
