import requests

headers = {'Accept': 'application/json'}

#API documentation:
#https://opendata.smhi.se/apidocs/metobs/stationSet.html

url = 'https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/%d/station-set/all/period/latest-hour/data.json'

params = {
    1  : "t1h",
    2  : "tMeanDay",
    3  : "dd10min1h",
    4  : "ff10min1h",
    5  : "rrDay",
    6  : "rh1h",
    7  : "rr1h",
    8  : "snowDay",
    9  : "ppp1h",
    10 : "sd1h",
    11 : "glir1h",
    12 : "sieve1h",
    13 : "ww1h",
    14 : "rr15min",
    15 : "rrint",
    16 : "n1h",
    17 : "rr6z18z",
    18 : "rr18z",
    19 : "tminDay",
    20 : "tmaxDay",
    21 : "cityFF",
    22 : "tMeanMonth",
    23 : "rrMonth",
    24 : "lwir1h",
    25 : "fx10min3h",
    26 : "tmin6z18z",
    27 : "tmax6z18z",
    28 : "cl1h",
    29 : "cla1h",
    30 : "cm1h",
    31 : "cma1h",
    32 : "ch1h",
    33 : "cha1h",
    34 : "cx1h",
    35 : "cxa1h",
    36 : "cb1h",
    37 : "cbm15min1h",
    38 : "rrintMax15min",
    39 : "td1h",
    40 : "ss6z"
}

for i in params:
    print(i)
    print(params[i])
    print(url % i)
    r = requests.get( url %i, headers=headers )
    try: print(f"Response: {r.json()}")
    except: continue
