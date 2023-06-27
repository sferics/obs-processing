import requests

headers = {'Accept': 'application/json'}

#API documentation:
#https://opendata.smhi.se/apidocs/metobs/stationSet.html
#TODO PARAMETERS dict
#https://opendata.smhi.se/apidocs/metobs/parameter.html

#url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/1/station/159880/period/latest-months.json"
url = 'https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/%d/station-set/all/period/latest-hour/data.json'

for i in range(1,41):
    print(i)
    print(url % i)
    r = requests.get( url %i, headers=headers )
    try: print(f"Response: {r.json()}")
    except: continue
