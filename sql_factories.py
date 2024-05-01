import copy
import pandas as pd
import polars as pl
import numpy as np
from collections import namedtuple
from datetime import datetime, date, time
# (lambda) function defitions for custom SQLite row and text factories

### ROW FACTORIES

# return as dictionary (borrowed from the python sqlite3 module documentation)
def dict_row(cursor, row):
    """
    """
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

# also taken from the documentation: https://docs.python.org/3/library/sqlite3.html#sqlite3-howto-row-factory
def named_row(cursor, row):
    """
    """
    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)

# return as pandas DataFrame
pandas_row          = lambda cursor, row : pd.DataFrame(row)

# return as polars DataFrame
polars_df_row       = lambda cursor, row : pl.DataFrame(row)

# return as polars LazyFrame
polars_lf_row       = lambda cursor, row : pl.LazyFrame(row)

# return as numpy array
numpy_row           = lambda cursor, row : np.asarray(row)

# return as set
set_row             = lambda cursor, row : {value for value in row} # or just set(row) ?

# return as list
list_row            = lambda cursor, row : [value for value in row] # or just list(row) ?

# for all above factories: return a list / array / df only if len > 1; else return single element
dict_len1_row       = lambda cursor, row : row[0] if len(row)==1 else dict_row(cursor, row)
named_len1_row      = lambda cursor, row : row[0] if len(row)==1 else named_row(cursor, row)
pandas_len1_row     = lambda cursor, row : row[0] if len(row)==1 else pandas_row(cursor, row)
polars_df_len1_row  = lambda cursor, row : row[0] if len(row)==1 else polars_df_row(cursor, row)
polars_lf_len1_row  = lambda cursor, row : row[0] if len(row)==1 else polars_lf_row(cursor, row)
numpy_len1_row      = lambda cursor, row : row[0] if len(row)==1 else numpy_row(cursor, row)
set_len1_row        = lambda cursor, row : row[0] if len(row)==1 else set_row(cursor, row)
list_len1_row       = lambda cursor, row : row[0] if len(row)==1 else list_row(cursor, row)
tuple_len1_row      = lambda cursor, row : row[0] if len(row)==1 else row

# sqlite3 default (if you ever want to reset your connection to default)
default_row         = lambda cursor, row : row # datatype will be tuple (implicit here)


### TEXT FACTORIES

# convert all pandas Datetime objects to python datetime
def pd2datetime_text(value):
    """
    """
    if type(value) == pd.Datetime:
        return value.to_pydatetime()
    else: return value

# convert all pandas Timestamp objects to integer timestmps (seconds since UNIX)
def pd2timestamp_text(value):
    """
    """
    if type(value) == pd.Timestamp:
        return int(pydatetime.timestamp())
    else: return value

# convert all datetime/date/time objects to integer timestamps (seconds since UNIX)
def datetime2timestamp_text(value):
    """
    """
    typ == type(value)
    if typ in {datetime, date, time}:
        if typ == date:
            value = datetime.fromordinal(value.ordinal())
        elif typ == time:
            today = datetime.today()
            value = datetime(today.year, today.day, time.hour, time.minute)
        return int(value.timestamp())
    else: return value

# enforce UTF8 decoding (default besides the errors='ignore'; python default is 'strict')
def utf8_text(value, errors="ignore"):
    """
    """
    return value.decode("utf-8", errors=errors)

# use latin1 decoding (iso8859-1 / Western Europe)
def latin1_text(value, errors="ignore"):
    """
    """
    return value.decode("latin-1", errors=errors)

# use ASCII decoding
def ascii_text(value, errors="ignore"):
    """
    """
    return value.decode("ascii", errors=errors)

# sqlite3 default (if you ever want to reset your connection to default)
def default_text(value):
    """
    """
    return value
