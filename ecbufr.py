#!/venv/bin/python3
#TODO: write bug report concerning memory leak to software.support@ecmwf.int

#decode bufr and save obs to database

#file lookup
from glob import glob
#sleep(seconds) function
from time import sleep
import numpy as np
#import pandas as pd
#python MySQL connector
#import MySQLdb
#regular expressions
import re
#for slicing dicts
#from itertools import islice
from datetime import datetime as dt, timedelta as td
#filesystem operations
from shutil import move, copyfileobj, copyfile
#read ini files
import configparser
#get command line arguments
import argparse
#bufr decoder by ECMWF
import eccodes as ec
#system modules
import psutil, os, sys
#writing dictionary to file, read it
import json
from write_files import write
from functions import total_size

#read config file
#config = configparser.ConfigParser()           
#config.read("config.ini")
#print(config)

#parser = argparse.ArgumentParser()

date = dt.now()
print( "ecdecode.py started @", date.strftime("%Y/%m/%d %H:%M") )

# Open database connection
#db = MySQLdb.connect("localhost", "obs", "obs4data", "obs" )

# prepare a cursor object using cursor() method
#cur = db.cursor()

bufr_dir      = "bufr/"
processed_dir = bufr_dir + "processed/"
files         = glob( bufr_dir + "Z__C_EDZW_*GER*bin" )
#files         = glob( bufr_dir + "*bin" )
#files          = glob( bufr_dir + "/GMA/*bin" )
output        = "output/"
data_json     = "data.json"

#list of known problematic desriptors (no tables available, not used etc)
#skip      = ["unexpandedDescriptors","stationNumber","blockNumber"]
skip      = ["unexpandedDescriptors"]
station_info = ["stationNumber","blockNumber","stationOrSiteName","shortStationName"]
null_vals = (2147483647, -1e+100, None, "None", "XXXX", {}, "", [])


def save_json(data, output= data_json ):
    #save DATA as JSON
    with open(output, 'w') as f:
        json.dump(data, f)

def write_output( INPUT= data_json, output_dir=output, mode="csv"):
    #write json data to file (csv or rpl)
    with open( INPUT ) as f:
        INPUT = json.load( f )
        write( INPUT, output_dir, mode="csv" )

clear = lambda keyname : re.sub( r"#[0-9]+#", '', keyname )

number = lambda keyname : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )

to_key = lambda number, clear_key : "#" + str(number) + "#" + clear_key


times = []
timekeys = ['year','month','day','hour','minute','timePeriod','timeSignificance','timeIncrement']
DATA = {}

#files = ["bufr/SYNOP/Z__C_EDZW_20210505181224_bda01,synop_bufr_999999_999999__MW_231.bin"]

attrs = ('code', 'units', 'scale', 'reference', 'width')

n_files = 0
n_loops = 0

stations = []
station_names = []

for FILE in files[:1]:

    time_start = dt.now()
    keys = []

    #alternative way to put the loop:
    #TODO is this actually faster?
    with ec.BufrFile(FILE) as bufr:
        # Print number of messages in file
        #print(len(bufr))
        # Open all messages in file
        for msg in bufr:
            items = msg.items()
            keys = msg.keys()
            si = {}

            for i in station_info:
                try: si[i] = msg[i]
                except: si[i] = None

            if si["shortStationName"] not in null_vals:
                stationID = "_" + str(si["shortStationName"])
            elif si["stationNumber"] not in null_vals:
                if blockNumber not in null_vals:
                    stationID = str(si["stationNumber"] + si["blockNumber"] * 1000)
                    while len(stationID) < 5:
                        stationID = "0" +  stationID
                else:
                    stationID = "00" + si["stationNumber"]
            else:
                try: stationID = si["stationOrSiteName"]
                except: stationID = ""#continue
            print(stationID) 
            
            print(keys)

            for key in keys:
                print(key)
                print(msg[key])

            #for item in items:
                #print(item)
                #for key in keys:
                    #print(msg[item][key])
