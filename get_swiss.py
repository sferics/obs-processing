#!/usr/bin/env python
import json, sys, os, shutil
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta as td, timezone as tz
from database import db; db = db()
from functions import read, ts2dt

config = read( "config" )

fmt  = "%Y-%m-%d@%H:%M"
fmt2 = "%Y%m%d%H%M"
latest_name = "VQHA80"
C = ".csv"
path = "SWISS/"

#create path directory if not exists
from pathlib import Path
Path(path).mkdir(parents=True, exist_ok=True)

if len(sys.argv) == 2:
    if sys.argv[1] == "-a":
        #take all .csv
        outfiles = glob(path + "*.csv")
        print(outfiles)
    else:
        outfiles = [path + sys.argv[1] + C]
else:
    # download latest csv to SWISS folder as "YYYY-MM-DD@hh:mm.csv" and "VQHA80.csv"
    import wget
    filename = latest_name + C
    url = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/"

    try: #remove oldest tmp file
        os.remove(path + filename)
    except FileNotFoundError:
        print("No latest file, downloading new!")
    #download latest file and copy it
    outfile = wget.download( url + filename, out = path )
    shutil.copyfile( outfile, path + dt.utcnow().strftime(fmt + C) )

stations = {
    "0-756-0-PFA" : "PFA",
    "0-756-0-BIA" : "BIA",
    "06679" : "TAE",
    "06756" : "COM",
    "06735" : "ABO",
    "06712" : "AIG",
    "06672" : "ALT",
    "06690" : "ARH",
    "06787" : "AND",
    "06695" : "ANT",
    "06785" : "ARO",
    "06686" : "RAG",
    "06634" : "BAN",
    "06601" : "BAS",
    "06642" : "LAT",
    "06631" : "BER",
    "06646" : "BEZ",
    "06721" : "BIN",
    "06678" : "BIZ",
    "06774" : "BIV",
    "06704" : "BIE",
    "06725" : "BLA",
    "06733" : "BOL",
    "06709" : "BOU",
    "06656" : "BRZ",
    "06633" : "BUS",
    "06778" : "BUF",
    "06619" : "FRE",
    "06752" : "CEV",
    "06674" : "CHZ",
    "06605" : "CHA",
    "06608" : "CHM",
    "06786" : "CHU",
    "06627" : "CHD",
    "06759" : "CIM",
    "06713" : "CDM",
    "06717" : "GSB",
    "06710" : "COY",
    "06688" : "CMA",
    "06606" : "CRM",
    "06784" : "DAV",
    "06602" : "DEM",
    "06782" : "DIS",
    "06693" : "EBK",
    "06739" : "EGH",
    "06648" : "EGO",
    "06675" : "EIN",
    "06682" : "ELM",
    "06655" : "ENG",
    "06715" : "EVI",
    "06722" : "EVO",
    "06616" : "FAH",
    "06652" : "FLU",
    "06625" : "GRA",
    "06613" : "FRU",
    "06700" : "GVE",
    "06653" : "GES",
    "06657" : "GIH",
    "06685" : "GLA",
    "06749" : "GOR",
    "06632" : "GRE",
    "06744" : "GRH",
    "06758" : "GRO",
    "06728" : "GRC",
    "06668" : "GOS",
    "06626" : "GOE",
    "06750" : "GUE",
    "06621" : "GUT",
    "06624" : "HLL",
    "06689" : "HOE",
    "06789" : "ILZ",
    "06734" : "INT",
    "06730" : "JUN",
    "06635" : "KOP",
    "06617" : "BRL",
    "06612" : "CDF",
    "06702" : "DOL",
    "06665" : "LAC",
    "06638" : "LAG",
    "06609" : "MLS",
    "06666" : "LEI",
    "06723" : "ATT",
    "06703" : "CHB",
    "06714" : "DIA",
    "06614" : "MAR",
    "06760" : "OTL",
    "06770" : "LUG",
    "06650" : "LUZ",
    "06669" : "LAE",
    "06762" : "MAG",
    "06640" : "MAS",
    "06618" : "MAH",
    "06754" : "MTR",
    "06637" : "MER",
    "06615" : "MOB",
    "06724" : "MVE",
    "06777" : "GEN",
    "06747" : "MRP",
    "06644" : "MOA",
    "06716" : "MTE",
    "06641" : "MOE",
    "06636" : "MUB",
    "06799" : "NAS",
    "06639" : "NAP",
    "06604" : "NEU",
    "06705" : "CGI",
    "06649" : "OBR",
    "06676" : "AEG",
    "06708" : "ORO",
    "06797" : "BEH",
    "06610" : "PAY",
    "06791" : "COV",
    "06795" : "PMA",
    "06628" : "PLF",
    "06794" : "ROB",
    "06711" : "PUY",
    "06687" : "QUI",
    "06751" : "ROE",
    "06645" : "RUE",
    "06783" : "SBE",
    "06623" : "HAI",
    "06792" : "SAM",
    "06662" : "SAG",
    "06620" : "SHA",
    "06790" : "SRS",
    "06683" : "SCM",
    "06651" : "SPF",
    "06798" : "SCU",
    "06779" : "SIA",
    "06654" : "SIM",
    "06720" : "SIO",
    "06706" : "PRE",
    "06600" : "STC",
    "06681" : "STG",
    "06796" : "SMM",
    "06771" : "SBO",
    "06671" : "STK",
    "06680" : "SAE",
    "06731" : "THU",
    "06740" : "TIT",
    "06677" : "UEB",
    "06745" : "ULR",
    "06990" : "VAD",
    "06793" : "VAB",
    "06663" : "VLS",
    "06603" : "VEV",
    "06788" : "VIO",
    "06707" : "VIT",
    "06727" : "VIS",
    "06780" : "WFJ",
    "06643" : "WYN",
    "06673" : "WAE",
    "06647" : "PSI",
    "06748" : "ZER",
    "06664" : "REH",
    "06660" : "SMA",
    "06670" : "KLO"
}

conflict = config["database"]["conflict"]

for outfile in outfiles:

    file_name = outfile.split("/")[-1]

    if not db.file_exists( file_name, path ):
        file_date = ts2dt( Path(outfile).stat().st_mtime )
        #set file status = locked and get rowid (FILE ID)
        try: ID = db.register_file(file_name, path, "SWISS", status="locked", creation_date=file_date)
        except Exception as e:
            if verbose: print(e)
            continue
    else:
        ID = db.get_file_id( file_name, path )
        #if file status is 'locked' continue with next file
        if db.get_file_status( ID ) == "locked": continue    

    # now we can work with the downloaded file (extract observations)
    df = pd.read_csv(outfile, sep=";")

    #the first to columns are station and date; skip those!
    params = df.columns[2:]

    #add SQL columns
    for p in params: db.add_column( "obs", p )

    obs = {}

    for s in stations:
        data = df.loc[df['Station/Location'] == stations[s]]
        for p in params:
            try:
                value = float(data[p].iloc[0])
            except (ValueError,IndexError):
                value = 'NULL'
            try:
                datestr = str(int(data["Date"].iloc[0]))
            except: continue
            
            hhmm = datestr[8:]

            obs[p] = value
            obs["stID"]   = s
            obs["file"]   = ID
            obs["prio"]   = -1
            obs["year"]   = int(datestr[:4])
            obs["month"]  = int(datestr[4:6])
            obs["day"]    = int(datestr[6:8])
            obs["hour"]   = int(hhmm[:2])
            obs["minute"] = int(hhmm[2:])

        db.sql_insert( "obs", obs, conflict=conflict ) 
    
    db.set_file_status( ID, "processed" )
    db.commit()

# commit and close db
db.close()
