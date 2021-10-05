#!/venv/bin/python3

#decode bufr and save obs to database

#file lookup
from glob import glob
from time import sleep
import numpy as np
import pandas as pd
#python MySQL connector
#import MySQLdb
#regular expressions
import re
#for slicing dicts
from itertools import islice
from datetime import datetime as dt, timedelta as td
#filesystem operations
from shutil import move, copyfileobj
import configparser
#bufr decoder by ECMWF
import pdbufr as pb
#system and path modules
import psutil, os, sys, pathlib, gzip, subprocess

#read config file
#config = configparser.ConfigParser()		
#config.read("config.ini")
#print(config)

date = dt.now()
print( date.strftime("%Y/%m/%d %H:%M") )

# Open database connection
#db = MySQLdb.connect("localhost", "obs", "obs4data", "obs" )

# prepare a cursor object using cursor() method
#cur = db.cursor()

bufr_dir      = "bufr/SYNOP/"
processed_dir = bufr_dir + "processed/"
files         = glob( bufr_dir + "Z__C*GER*bin" )
#files         = glob( bufr_dir + "*bin" )
output        = "output/"

#list of known problematic desriptors (no tables available, not used etc)
skip        = ("unexpandedDescriptors","stationNumber","blockNumber")
null_values = (2147483647, -1e+100, None, "None", "XXXX", {}, "", [])


clear = lambda keyname : re.sub( r"#[0-9]+#", '', keyname )

number = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )

to_key = lambda number, clear_key : "#" + str(number) + "#" + clear_key

to_C = lambda kelvin : int((kelvin - 273.15)*10)

to_kt = lambda ms : int( (900/463) * float(ms) )

to_hPa = lambda Pa : np.round( Pa/100, 1 )

def to_type(station):
    d = {0: "SYNO"}
    return d[station]

def to_lat(lat):
    celestial = "N" if float(lat) > 0 else "S"
    deg = str(abs(int(lat)))
    while len(deg) < 2:
        deg = "0" + deg
    minutes = str( abs(int( np.round((lat-int(lat))*100, 2)) ) )
    return celestial + deg + " " + minutes

def to_lon(lon):
    celestial = "E" if float(lon) > 0 else "W"
    deg = str(abs(int(lon)))
    while len(deg) < 3:
        deg = "0" + deg
    minutes = str( abs(int( np.round((lon-int(lon))*100, 2)) ) )
    return celestial + deg + " " + minutes

#pseudo function which does nothing, if value has to remain unchanged
n = lambda x : x


def search(dictionary, substr, sort=True):
    result = []
    for key in dictionary:
        if substr in key:
            result.append(key)
    if sort: return sorted(result)
    return result


def write( data, output, mode="csv", keyorder=False ):

    times = []

    for DATE in data:
        print(DATE)
        #create folder for DATE if not exists
        pathlib.Path(output + "bufr" + DATE).mkdir(parents=True, exist_ok=True)
        
        #write data for each station and date in folders
        for station in data[DATE]:
            
            filename = output + "bufr" + DATE + "/bufr" + station + ".csv.gz"
            
            #remove file if exists
            if os.path.exists(filename):
                os.remove(filename)

            time_start = dt.now()

            with gzip.open( filename, "wt", encoding="utf8" ) as o:
                for hour in data[DATE][station]:
                    print(data[DATE][station][hour].keys())
                    timecode = DATE + hour + "00"
                    print(timecode)
                    line    = []
                    line.append( station )
                    #INDEX;LOCATOR;TYPE;C;NAME                          ;LAT   ;LON    ;ELEV;YYYYMMDDhhmm;DIR;K; FF; FG;G1h;G10; FX;FX1;FFM;  TL;  TD;  T5;  TX;  TN; TX6; TN6; TX1; TN1; TN5;  TW;Tsfc;T-05;T-10;T-20;T-50;T-1m;T-2m;   QFE;   QFF; QNH;  AP;A;www;960;961;962;963;           MetarWX;W1;W2;     WWZ; LTNG; VIS;COV;N;L;HLC;   ClCmCh;LAYER1;LAYER2;LAYER3;LAYER4;CLG;     PIC;VCLOUD1;VCLOUD2;VCLOUD3;VCLOUD4;  VFOG;   RR;TR;RR10m; RR1h;hh;RR24h;WRTR;SNO;NEW;SR;GS;SS24;Sh;S10;GL24;DF24;LW24;GL1h;DF1h;LW1h;GL10;DF10;LW10;  HW;PW;DW1; HW1;P1;DW2; HW2;P2;  HS;PS;I;B;
                    
                    params  = ["locator","stationType","","stationOrSiteName","latitude","longitude","elevation","timecode", \
                            "windDirection","K","windSpeed","maximumWindGustSpeed","G1h","G10","FX","FX1","FFM","airTemperature", \
                            "dewpointTemperature","soilTemperature","maximumTemperatureAtHeightAndOverPeriodSpecified", \
                            "minimumTemperatureAtHeightAndOverPeriodSpecified","TX6","TN6","TX1","TN1","TN5","TW","Tsfc","T-05", \
                            "T-10","T-20","T-50","T-1m","T-2m","QFE","QFF","QNH","AP","A","www","960","962","963","MetarWX", \
                            "W1","W2","WWZ","LTNG","VIS","COV","N","L","HLC"]
                    params += (90-len(params))*[""]
                    print(len(params))
                    #80
                    
                    units   = [n,to_type,n,n,to_lat,to_lon,n,n,n,n,to_kt,to_kt,n,n,n,n,n,to_C,to_C,to_C,n,n,n,n,n,n,n,n,n]
                    minutes = [0,0,0,0,0,0,0,0,0,0,0,0,0,20]

                    for i in range(len(params)):
                        try:
                            params[i] = units[i]( data[DATE][station][hour][mins][params[i]] )
                        except:
                            params[i] = "/"
                        if params[i] in null_values:
                            params[i] = "/"

                    print(params)

                    formats = [":>7",":>4",":>1",":<30",":>6",":>6",":>4",":>12",":>3",":>1"] +7*[":>3"] +11*[":>4"]+62*[":>1"]
                    print(len(formats))
                    for i in range(60):
                        units.append(n)

                    for i in (params, formats):
                        print(len(i))

                    for p, f in zip(params, formats):
                        line.append( ("{"+f+"}").format( p ) )

                    line.append("")
                    
                    print(line)
                    print(";".join(line) + "\n")

                    o.write( ";".join(line) + "\n" )
                
                o.close()
        
            time_end = dt.now()
            time_diff = float(str(time_end - time_start )[-9:])
            times.append(float(time_diff))
            os.system("clear")
            if len(times) > 0:
                print("max:    %1.3f s" % np.round( np.max(times), 3) )
                print("min:    %1.3f s" % np.round( np.min(times), 3) )
                print("mean:   %1.3f s" % np.round( np.mean(times), 3) )
                print("median: %1.3f s" % np.round( np.median(times), 3) )


times = []
timekeys = ('year', 'month', 'day', 'hour', 'minute', 'timePeriod', 'timeSignificance', 'timeIncrement')
DATA = {}

#files = ["bufr/SYNOP/Z__C_EDZW_20210505181224_bda01,synop_bufr_999999_999999__MW_231.bin"]

attrs = ('code', 'units', 'scale', 'reference', 'width')

#create empty pandas dataframe
#df0 = pd.DataFrame()

keys = ("data_datetime", "stationNumber", "stationType","stationOrSiteName","latitude","longitude","elevation", \
                            "windDirection","windSpeed","maximumWindGustSpeed","airTemperature", \
                            "dewpointTemperature","soilTemperature","maximumTemperatureAtHeightAndOverPeriodSpecified", \
"minimumTemperatureAtHeightAndOverPeriodSpecified")

keys = ("data_datetime", "stationNumber", "latitude","longitude","elevation")

i = 0

for FILE in files[:100]:
    
    time_start = dt.now()

    if i > 0:
        df0 = df.copy()
    
    try:
        df = pb.read_bufr( FILE, columns = keys, filters={'stationNumber': ["381"] } )
        print(df)
        i += 1
    except: continue
    #concatenate existing (in first iter empty) with new dataframe

    if i > 1 and "df0" in vars() or "df0" in globals():
        df = pd.concat([df0, df])

    time_end = dt.now()
    time_diff = float(str(time_end - time_start )[-9:])
    times.append(float(time_diff))
    
    #move file to processed folder
    #move( FILE, processed_dir + FILE.replace(bufr_dir, "") )

    """
    os.system("clear")
    if len(times) > 0:
        print("max:    %1.3f s" % np.round( np.max(times), 3) )
        print("min:    %1.3f s" % np.round( np.min(times), 3) )
        print("mean:   %1.3f s" % np.round( np.mean(times), 3) )
        print("median: %1.3f s" % np.round( np.median(times), 3) )    
   
    process = psutil.Process(os.getpid())
    memory_used = process.memory_info().rss // 1024 // 1024
    memory_free = psutil.virtual_memory()[1] // 1024 // 1024
    print("--------------------")
    print("Memory used: %4d MB" % memory_used )  # in megabytes
    print("Memory free: %4d MB" % memory_free )
    """
    
    #if less than 1 GB free memory
    if psutil.virtual_memory()[1] // 1024 // 1024 < 1024:
        last_file = np.copy(FILE)
        print("Too much RAM used!")
        print(last_file)
        break
        #TODO: restart automatically

print(df)

print("%d files decoded" % len(files) )

print("writing data...")
#write(DATA, output)
print("writing finished!")

#db.commit()
#cur.close()
#db.close()
