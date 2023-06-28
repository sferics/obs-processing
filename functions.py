def read_file(file_name):
    from pathlib import Path
    return Path( file_name ).read_text()

def read_yaml(file_path):
    with open(file_path, "r") as f:
        import yaml
        return yaml.load(f, yaml.Loader)

def get_file_path( FILE, string=True ):
    import Path
    PATH = Path( FILE ).resolve().parent
    if string:
        return str( PATH )
    return PATH

def get_file_date( file_path, datetime=True ):
    date = Path(file_path).stat().st_mtime
    if datetime:
        return ts2dt( date )
    return date

from datetime import datetime as dt, timezone as tz
import numpy as np

def dt2str( datetime, fmt ):
   """datetime -> string"""
   datetime_str = datetime.strftime( fmt )
   return datetime_str

def dt2ts( datetime, min_time = False, tzinfo=tz.utc ):
   """convert today's datetime object to timestamp"""
   if min_time: dtts = dt.combine( datetime, dt.min.time() )
   else: dtts = datetime
   return int( dtts.replace( tzinfo = tz.utc ).timestamp() )

def str2dt( string, fmt, tzinfo=tz.utc ):
   """convert string to datetime object"""
   datetime = dt.strptime(string, fmt).replace( tzinfo=tzinfo )
   return datetime

def str2ts( string, fmt, min_time = False, tzinfo=tz.utc ):
   """string -> timestamp"""
   datetime = str2dt( string, fmt )
   return dt2ts( datetime, min_time = min_time, tzinfo=tzinfo )

ts2dt = lambda ts : dt.fromtimestamp( ts )

hhmm_str = lambda integer : str(integer).rjust(2, "0")

class clock_iter:
   """Iterator class; adds 10 mins to the iterated variable"""
   def __init__(self, start="0000"):
      self.hh = start[0:2]; self.mm = start[2:]; self.time = start
   def __iter__(self):
      return self
   def __next__(self):
      if self.time == "2350":
         self.hh = "00"; self.mm = "00"; self.time = "0000"
         return self.time
      else: #for all other times
         if self.mm == "50":
            self.hh = hhmm_str( int(self.hh)+1 )
            self.mm = "00"
            self.time = self.hh + self.mm
            return self.time
         else: #self.hh remains unchanged!
            self.mm = str( int(self.mm)+10 )
            self.time = self.hh + self.mm
            return self.time

from os import getpid
from os.path import exists
from psutil import pid_exists, Process

def already_running( pid_file = "pid.txt" ):
    #https://stackoverflow.com/a/73363976
    if exists( pid_file ):
        return True
    with open( pid_file, 'w' ) as f:
        f.write( str(getpid()) )
    return False

def already_running2():
    """same as above but without pid file"""
    import subprocess, sys
    
    cmd = [f'pgrep -f .*python.*{sys.argv[0]}']
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
    my_pid, err = process.communicate()

    if len(my_pid.splitlines()) >0:
       return True
    return False
