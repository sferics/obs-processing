**QUESTIONS TO DISCUSS IN BBB**
- forging chain a good idea? [reduce, derive, aggregate, derive (only 30-min values), audit, emtpy]
- deriving elements two times necessary ??? what if derived TMP, DPT and RH do not match timewise??
- only aggregate over durations that we use in the current system? aggregate\_obs.py is pretty slow
> ANSWER: YES, we actually do not need more and are focussing on intergration with current system !
- RR24 (PRATE24h\_1m\_syn) defined from 0z to 24z (only) OR from 6z (obs day) to 6z (next day) ???
> ANSWER: [vorticity.de](#https://vorticity.de/_ds/ds_mw_9002_r24hi_glo_week.html)
- current DEV priority: dev OR oper ??? jump checks OR derivation of imported metwatch obs ???
- next DEV priority? decode METAR/NetCDF, export\_obs.py, get\_obs.py, add\_stations\_from\_bufr.py
- JUMP CHECK implementation in audit\_obs.py? check original implementation in MSwr operational MOS
