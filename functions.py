def read_file(file_name):
    from pathlib import Path
    return Path( file_name ).read_text()

def read_yaml(file_path):
    with open(file_path, "r") as f:
        import yaml
        return yaml.load(f, yaml.Loader)


def search(dictionary, substr, sort=True):

    result = []
    for key in dictionary:
        if substr in key:
            result.append(key)
    if sort: return sorted(result)
    return result

from sys import getsizeof, stderr
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)

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
