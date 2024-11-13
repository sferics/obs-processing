import sys, re, inspect, sqlite3 # sqlite connector python base module
from contextlib import contextmanager
import global_functions as gf
import global_variables as gv
from datetime import datetime as dt
#import sql_factories as sf

class DatabaseClass:
    
    #@classmethod
    def __init__(self, db_file="main.db", config={ "timeout":5, "log_level":"NOTSET", "verbose":0,
            "traceback":0, "settings":{} }, text_factory=None, row_factory=None, ro=False):
        """
        """
        # make config accessible as class object
        self.config = config

        #TODO add logging statements where it makes sense for debugging/monitoring of database activities
        if "log_level" in config and config["log_level"] in gv.log_levels:
            self.log_level = config["log_level"]
        else: self.log_level = "NOTSET"
       
        # get a logger object which we can call with self.log.info for example
        self.log = gf.get_logger( self.__class__.__name__, self.log_level )

        # keep name of datebase file which we want to attach (connect)
        self.db_file = db_file
        
        try:    self.timeout    = float(config["timeout"])
        except: self.timeout    = 5 # sqlite default value
        
        self.verbose    = config.get("verbose")
        self.traceback  = config.get("traceback")
        
        # if ro == True we open the database in read-only mode
        if ro: self.db_file = f"file:{self.db_file}?mode=ro"

        # Opening database connection (uri needs to be also True if read-only)
        self.con        = sqlite3.connect(self.db_file, timeout=self.timeout, uri=ro)
        
        if callable(row_factory): self.con.row_factory = row_factory
        # else set to default (get rid of tuple w/ length 1)
        else: # all row and text factories are outsourced to a seperate file
            from sql_factories import tuple_len1_row
            self.con.row_factory = tuple_len1_row

        # use string converter
        if callable(text_factory): self.con.text_factory = text_factory
        
        # enable the custom REGEXP (regex) function
        def regexp(expr, item):
            reg = re.compile(expr)
            return reg.search(item) is not None
        
        # define it on the connection object
        self.con.create_function("REGEXP", 2, regexp)
        
        # Set up database cursor
        self.cur        = self.con.cursor()
        # shorthand for cursor rowcount
        self.rowcnt     = self.cur.rowcount
        # shorthand for lastrowid
        self.lastid     = self.cur.lastrowid
        # shorthand for description (column names)
        self.descr      = self.cur.description
        # alias / shorthand for arraysize (number of rows to return by fetchmany() by default)
        self.n_rows     = self.cur.arraysize
        
        # make row and text factory accessible after initiating database object\
        self.row_factory    = self.con.row_factory
        self.text_factory   = self.con.text_factory
        
        # apply all PRAGMA settings from the settings dictionary
        if "settings" in config:
            settings = config["settings"]
            for i in settings:
                # if setting i is set: change to new value
                if settings[i]: setattr(self, i, settings[i])
                # elif verbose: print current setting
                elif self.verbose: print( i, setting )
    
        #TODO register adapters and converters for custom datatypes defining how they are stored
        #https://docs.python.org/3.11/library/sqlite3.html#sqlite3.register_adapter
    
    # some useful shorthand definitions
    commit      = lambda self         : self.con.commit()
    fetch1      = lambda self         : self.cur.fetchone()
    fetchmany   = lambda self, n_rows : self.cur.fetchmany(n_rows)
    fetch       = lambda self         : self.cur.fetchall()
    exe         = lambda self, *param : self.cur.execute(*param)
    exemany     = lambda self, *param : self.cur.executemany(*param)
    exescr      = lambda self, *param : self.cur.executescript(*param)
    attach      = lambda self, DB, AS : self.exe( f"ATTACH DATABASE '{DB}' as '{AS}'" )
    detach      = lambda self, DB     : self.exe( f"DETACH DATABASE '{DB}'" )
    
    # let the cursor iterate over result rows and yield them
    # inspired by: thttps://stackoverflow.com/questions/18712772/how-to-return-a-generator-in-python
    def cursor_iter(self, cur, chunks=None):
        if chunks is None:
            rows = cur.fetchall()
            for row in rows:
                yield row
        else:
            rows = chunks
            while rows:
                rows = cur.fetchmany(rows)
                if not rows: break
                for row in rows:
                    yield row
     
    # return rows as a generator (useful to create a DataFrame from them)
    #@contextmanager
    def fetch_gt(self, cur, *param, chunks=None):
        cur.execute(*param)
        return self.cursor_iter(cur, chunks)
    
    def fetch_gt_as(self, cur, aswhat, *param, chunks=None):
        gt = self.fetch_gt(cur, *param, chunks=chunks)
        return aswhat(gt)
     
    def fetch_pandas_df(self, cur, *param, chunks=None):
        from pandas import DataFrame
        return self.fetch_gt_as(cur, DataFrame, *param, chunks=chunks)
    
    def fetch_polars_df(self, cur, *param, chunks=None):
        from polars import DataFrame
        return self.fetch_gt_as(cur, DataFrame, *param, chunks=chunks)
    
    def fetch_polars_lf(self, cur, *param, chunks=None):
        from polars import LazyFrame
        return self.fetch_gt_as(cur, LazyFrame, *param, chunks=chunks)
    
    def attach_station_db(self, loc, output, mode="dev", stage="forge"):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        ------
        """
        from obs import ObsClass as oc
        station_db_path = oc.get_station_db_path(loc, output, mode, stage)
        self.attach(station_db_path, stage)


    def detach_station_db(self, loc, output, mode="dev", stage="forge"):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        ------
        """
        from obs import ObsClass as oc
        station_db_path = oc.get_station_db_path(loc, output, mode, stage)
        self.detach(station_db_path, stage)


    def close(self, commit=True, verbose=False ):
        """
        Parameter:
        ----------
        commit : commit database after closing the connection to database and cursor
        
        Notes:
        ------
        Closes the database connection and cursor of the current database object.
        
        Return:
        ------
        True if succesfully cloesd (and committed), False if not
        """
        try:
            if commit: self.commit()
            self.cur.close()
            self.con.close()
        except Exception as e:
            if verbose: gf.print_trace(e)
            return False
        return True


    def pragma( self, pragma, args=""):
        """
        simple pragma function inspired by aspw (SQLite wrapper, https://github.com/rogerbinns/apsw)
        """
        if args: args = " " + args
        self.exe( f"PRAGMA {pragma}{args}" )


    # generic getter/setter PRAGMA function. can be called by actual pragma functions, which follow
    def pragma_get_set( self, pragma, N=None, schema="" ):
        """
        Parameter:
        ----------
        pragma : name of PRAGMA statement (https://www.sqlite.org/pragma.html)
        N : settting

        Notes:
        ------
        query or set given PRAGMA
        https://www.sqlite.org/pragma.html#pragma_auto_vacuum

        Return:
        -------
        setting if N not provided, else None
        """
        if N is not None: N = f" = {N}"
        else: N = ""
        if schema: schema = f"'{schema}'."
        self.exe( f"PRAGMA {schema}{pragma}{N}" )
        if not N: return self.fetch1()


    def analysis_limit( self, N="" ):
        """
        Parameter:
        ----------
        N : int setting the limit of the ANALYZE setting

        Notes:
        ------
        Query or change a limit on the approximate ANALYZE setting.
        https://www.sqlite.org/pragma.html#pragma_analysis_limit

        Return:
        -------
        setting if N not provided, else None
        """
        # inspect.currentframe().f_code.co_name gets the name of the current function 
        # found it here: https://www.geeksforgeeks.org/python-how-to-get-function-name/
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )

    
    def auto_vacuum( self, N="" ):
        """
        Parameter:
        ----------
        N : setttings = [ 0 | NONE | 1 | FULL | 2 | INCREMENTAL ]

        Notes:
        ------
        query or set the auto-vacuum status in the database
        https://www.sqlite.org/pragma.html#pragma_auto_vacuum

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )
   

    def automatic_index( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : a bool turning automatic indexing on or off

        Notes:
        ------
        Query or set automatic indexing in the database
        https://www.sqlite.org/pragma.html#pragma_automatic_index

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean ) 
   

    def busy_timeout( self, ms="" ):
        """
        Parameter:
        ----------
        ms: int setting the timeout in milliseconds

        Notes:
        ------
        Query or change the setting of the busy timeout.
        https://www.sqlite.org/pragma.html#pragma_busy_timeout
        
        Return:
        -------
        setting if ms not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, ms )
   

    def cache_size( self, N="" ):
        """
        Parameter:
        ----------
        N : If the argument N is positive then the suggested cache size is set to N. If the argument N is
        negative, then the number of cache pages is adjusted to be a number of pages that would use approximately
        abs(N*1024) bytes of memory based on the current page size.

        Notes:
        ------
        query or change the suggested maximum number of database disk pages that SQLite will hold in memory at
        once per open database file
        https://www.sqlite.org/pragma.html#pragma_cache_size

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )

    
    def cache_spill( self, boolean=None, schema="" ):
        """
        Parameter:
        ----------
        boolean : bool setting case sensitive like on/off for current database object
        schema : str schema (database name)

        Notes:
        ------
        query or change ability of pager to spill dirty cache pages to database file in the middle of transaction
        https://www.sqlite.org/pragma.html#pragma_cache_spill

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=boolean, schema=schema )
   

    def case_sensitive_like( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool setting case sensitive like on/off for current database object

        Notes:
        ------
        query or change the behaviour of the LIKE operator
        https://www.sqlite.org/pragma.html#pragma_case_sensitive_like

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )

    
    def cell_size_check( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool setting cell size check on/off for current database object

        Notes:
        ------
        query or change additional sanity checking on database b-tree pages
        https://www.sqlite.org/pragma.html#pragma_cell_size_check

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def defer_foreign_keys( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool setting defer foreign keys on/off for current database object

        Notes:
        ------
        query or change enforcement of all foreign key constraints
        https://www.sqlite.org/pragma.html#pragma_defer_foreign_keys

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )

    
    def encoding( self, N="" ):
        """
        Parameter:
        ----------
        N : encoding [UTF-8", UTF-16, UTF-16le, UTF-16be ]

        Notes:
        ------
        query or set the encoding of the database
        https://www.sqlite.org/pragma.html#pragma_encoding

        Return:
        -------
        encoding of database if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )


    def foreign_keys( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool setting foreign keys on/off for current database object

        Notes:
        ------
        query, set, or clear the enforcement of foreign key constraints
        https://www.sqlite.org/pragma.html#pragma_defer_foreign_keys

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def hard_heap_limit( self, N="" ):
        """
        Parameter:
        ----------
        N : setting

        Notes:
        ------
        query or invoke the sqlite3_hard_heap_limit64() interface with the argument N (a non-negative integer)
        https://www.sqlite.org/pragma.html#pragma_hard_heap_limit

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )


    def ignore_check_constraints( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling ignore check constraints for current database object

        Notes:
        ------
        query, enable or disable the enforcement of CHECK constraints
        https://www.sqlite.org/pragma.html#pragma_ignore_check_constraints

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def journal_mode( self, N="", schema="" ):
        """
        Parameter:
        ----------
        N : setting [ DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF ]
        schema : database name

        Notes:
        ------
        query or set the journal mode for databases associated with the current database connection
        https://www.sqlite.org/pragma.html#pragma_journal_mode

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=N, schema=schema )


    def journal_size_limit( self, N="", schema="" ):
        """
        Parameter:
        ----------
        N : setting
        schema : database name

        Notes:
        ------
        query or set journal_size_limit
        https://www.sqlite.org/pragma.html#journal_size_limit

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=N, schema=schema )


    def legacy_alter_table( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable legacy_alter_table
        https://www.sqlite.org/pragma.html#legacy_alter_table

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def locking_mode( self, N="" ):
        """
        Parameter:
        ----------
        N : setting [ NORMAL | EXCLUSIVE ]

        Notes:
        ------
        query, enable or disable locking_mode
        https://www.sqlite.org/pragma.html#locking_mode

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )


    def max_page_count( self, N="", schema="" ):
        """
        Parameter:
        ----------
        N : setting
        schema : database name

        Notes:
        ------
        query or set max_page_count
        https://www.sqlite.org/pragma.html#max_page_count

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=N, schema=schema )


    def mmap_size( self, N="", schema="" ):
        """
        Parameter:
        ----------
        N : setting
        schema : database name

        Notes:
        ------
        query or set mmap_size
        https://www.sqlite.org/pragma.html#mmap_size

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=N, schema=schema )


    def page_size( self, boolean="", schema="" ):
        """
        Parameter:
        ----------
        boolean : setting
        schema : database name

        Notes:
        ------
        query or change page_size
        https://www.sqlite.org/pragma.html#page_size

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=boolean, schema=schema )


    def parser_trace( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable parser_trace
        https://www.sqlite.org/pragma.html#parser_trace

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def query_only( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable query_only
        https://www.sqlite.org/pragma.html#query_only

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def read_uncommitted( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable read_uncommitted
        https://www.sqlite.org/pragma.html#read_uncommitted

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def recursive_triggers( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable recursive_triggers
        https://www.sqlite.org/pragma.html#recursive_triggers

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def recursive_triggers( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable recursive_triggers
        https://www.sqlite.org/pragma.html#recursive_triggers

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def reverse_unordered_selects( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool enabling/disabling setting

        Notes:
        ------
        query, enable or disable reverse_unordered_selects
        https://www.sqlite.org/pragma.html#reverse_unordered_selects

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def schema_version( self, integer="", schema="" ):
        """
        Parameter:
        ----------
        integer : setting
        schema : database name

        Notes:
        ------
        query or change schema_version
        https://www.sqlite.org/pragma.html#schema_version

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=integer, schema=schema )


    def secure_delete( self, boolean="", schema="" ):
        """
        Parameter:
        ----------
        boolean : setting
        schema : database name

        Notes:
        ------
        query or change secure_delete
        https://www.sqlite.org/pragma.html#secure_delete

        Return:
        -------
        setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=boolean, schema=schema )


    def soft_heap_limit( self, N=None ):
        """
        Parameter:
        ----------
        N : soft heap limit

        Notes:
        ------
        query or invoke the sqlite3_soft_heap_limit64() interface with the argument N (a non-negative integer)
        https://www.sqlite.org/pragma.html#pragma_soft_heap_limit

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )


    def synchronous( self, N=None ):
        """
        Parameter:
        ----------
        N : setting [ 0 | OFF | 1 | NORMAL | 2 | FULL | 3 | EXTRA ]

        Notes:
        ------
        query or change the synchronous setting
        https://www.sqlite.org/pragma.html#synchronous

        Return:
        -------
        setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )

    
    def temp_store( self, N=None ):
        """
        Parameter:
        ----------
        N : temp_store setting [ 0, DEFAULT, 1, FILE, 2, MEMORY ]

        Notes:
        ------
        Query or change the setting of the "temp_store" parameter
        https://www.sqlite.org/pragma.html#pragma_temp_store

        Return:
        -------
        temp_store setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )


    def threads( self, N=None ):
        """
        Parameter:
        ----------
        N : int number of sqlite worker threads

        Notes:
        ------
        Query or change the value of the sqlite3_limit thread limit for the current database connection.
        https://www.sqlite.org/pragma.html#pragma_threads
        
        Return:
        -------
        thread setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )

    
    def trusted_schema( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool changing the trusted schema setting

        Notes:
        ------
        Query or change the trusted_schema setting which is a per-connection boolean that determines whether or
        not SQL functions and virtual tables that have not been security audited are allowed to be run by views,
        triggers, or in expressions of the schema such as CHECK constraints, DEFAULT clauses, generated columns
        expression indexes, and/or partial indexes.
        https://www.sqlite.org/pragma.html#pragma_trusted_schema

        Return:
        -------
        trusted_schema setting if boolean not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def user_version( self, schema=None, integer=None ):
        """
        Parameter:
        ----------
        integer : int changing the user version

        Notes:
        ------
        get or set the value of the user-version integer at offset 60 in the database header
        https://www.sqlite.org/pragma.html#pragma_user_version

        Return:
        -------
        user_version if integer not given, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N=integer, schema=schema )


    def wal_autocheckpoint( self, N=None ):
        """
        Parameter:
        ----------
        N : int setting write-ahead log auto-checkpoint interval

        Notes:
        ------
        queries or sets the write-ahead log auto-checkpoint interval
        https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoints
        
        Return:
        -------
        wal_autocheckpoint setting if N not provided, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, N )

    
    def writable_schema( self, boolean=None ):
        """
        Parameter:
        ----------
        boolean : bool or RESET

        Notes:
        ------
        Query or change the writable schema setting. When this pragma is on, and the SQLITE_DBCONFIG_DEFENSIVE
        flag is off, the sqlite_schema table can be changed using ordinary UPDATE, INSERT, and DELETE statements.
        https://www.sqlite.org/pragma.html#pragma_writable_schema

        Return:
        -------
        writable_schema setting if boolean not given, else None
        """
        self.pragma_get_set( inspect.currentframe().f_code.co_name, boolean )


    def collation_list( self ):
        """
        Return:
        -------
        a list of the collating sequences defined for the current database connection.
        https://www.sqlite.org/pragma.html#pragma_collation_list
        """
        self.exe("PRAGMA collation_list")
        return self.fetch()


    def data_version( self, schema="" ):
        """
        Parameter:
        ----------
        schema : database schema/name
        
        Notes:
        provides an indication that the database file has been modified
        https://www.sqlite.org/pragma.html#pragma_data_version

        Return:
        -------
        integer value (consult documentation for further information)
        """
        if schema: schema = f"'{schema}'."
        self.exe("PRAGMA {schema}data_version")
        return self.fetch1()


    def database_list( self ):
        """
        Notes:
        ------
        This pragma works like a query to return one row for each database attached to the current database
        connection. The second column is "main" for the main database file, "temp" for the database file used to
        store TEMP objects, or the name of the ATTACHed database for other database files. The third column is
        the name of the database file itself, or an empty string if the database is not associated with a file.
        """
        self.exe( "PRAGMA database_list" )
        return self.fetch()


    def foreign_key_check( self, table_name="", schema="" ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_foreign_key_check
        """
        if table_name:  table_name  = f"({table_name})"
        if schema:      schema      = f"'{schema}'."
        self.exe( f"PRAGMA {schema}foreign_key_check{table_name}" )
        return self.fetch()


    def foreign_key_list( self, table_name="" ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_foreign_key_list
        """
        if table_name:  table_name  = f"({table_name})"
        self.exe( f"PRAGMA foreign_key_list{table_name}" )
        return self.fetch()


    def freelist_count( self, schema="" ):
        """
        Notes:
        ------
        return the number of unused pages in the database file
        """
        if schema:      schema      = f"'{schema}'."
        self.exe( f"PRAGMA {schema}freelist_count" )
        return self.fetch1()

    
    def function_list( self ):
        """
        Notes:
        ------
        This pragma returns a list of SQL functions known to the database connection.
        Each row of the result describes a single calling signature for a single SQL function.
        Some SQL functions will have multiple rows in the result set if they can (for example)
        be invoked with a varying number of arguments or can accept text in various encodings.
        """
        self.exe( "PRAGMA function_list" )
        return self.fetch()
   

    def incremental_vacuum( self, N="", schema="" ):
        """
        Notes:
        ------
        The incremental_vacuum pragma causes up to N pages to be removed from the freelist.
        The database file is truncated by the same amount. The incremental_vacuum pragma has no
        effect if the database is not in auto_vacuum=incremental mode or if there are no pages
        on the freelist. If there are fewer than N pages on the freelist, or if N is less than 1,
        or if the "(N)" argument is omitted, then the entire freelist is cleared.
        """
        if schema:      schema      = f"'{schema}'."
        if N:           N           = f"({N})"
        self.exe( f"PRAGMA {schema}incremental_vacuum{N}" )

    
    def index_info( self, index_name, schema="", hidden=False ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_index_info
        """
        if schema:      schema      = f"'{schema}'."
        if hidden:      return self.index_xinfo( index_name, schema )
        self.exe( f"PRAGMA {schema}index_info{index_name}" )
        return self.fetch()


    def index_list( self, table_name ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_index_list
        """
        if schema:      schema      = f"'{schema}'." 
        self.exe( f"PRAGMA {schema}index_list{index_name}" ) 
        return self.fetch()


    def index_xinfo( self, index_name ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_index_xinfo
        """
        if schema:      schema      = f"'{schema}'." 
        self.exe( f"PRAGMA {schema}index_xinfo{index_name}" )
        return self.fetch()


    def integrity_check( self, N_or_table, schema="" ):
        """
        see documentation: https://www.sqlite.org/pragma.html#pragma_integrity_check
        """
        if schema:      schema      = f"'{schema}'."
        self.exe( f"PRAGMA {schema}integrity_check({N_or_table})" )
        return self.fetch()


    def module_list( self ):
        """
        This pragma returns a list of virtual table modules registered with the database connection.
        """
        self.exe( "PRAGMA module_list" )
        return self.fetch()


    def optimize( self, mask="", schema="" ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_optimize
        """
        if schema:      schema      = f"'{schema}'."
        if mask:        mask        = f"({mask})"
        self.exe( f"PRAGMA {schema}optimize({mask}" )
        

    def page_count( self, schema="" ):
        """
        Return the total number of pages in the database file.
        """
        if schema:      schema      = f"'{schema}'."
        self.exe( f"PRAGMA {schema}page_count" )
        return self.fetch1()


    def pragma_list( self ):
        """
        This pragma returns a list of PRAGMA commands known to the database connection.
        """
        self.exe( "PRAGMA pragma_list" )
        return self.fetch()


    def quick_check( self, N_or_table, schema="" ):
        """
        The pragma is like integrity_check except that it does not verify UNIQUE constraints and
        does not verify that index content matches table content. By skipping UNIQUE and index
        consistency checks, quick_check is able to run faster. PRAGMA quick_check runs in O(N) time
        whereas PRAGMA integrity_check requires O(NlogN) time where N is the total number of rows
        in the database. Otherwise the two pragmas are the same.
        """
        if schema:      schema      = f"'{schema}'."
        self.exe( f"PRAGMA {schema}quick_check({N_or_table})" )
        return self.fetch()


    def table_list( self, table_name=None, schema="" ):
        """
        Parameter:
        ----------
        schema: schema in which the table or view appears
        table_name : name of the table which we want to list across all databases

        Notes:
        ------        
        This pragma returns information about the tables and views in the schema, one table per row of output.
        https://www.sqlite.org/pragma.html#pragma_table_list
        """
        if table_name:
            self.exe( f"PRAGMA table_list('{table_name}')" )
            return self.fetch()
        else:
            if schema: schema = f"'{schema}'."
            sql = f"PRAGMA {schema}table_list"
            self.exe( sql )
            return self.fetch()


    def table_info( self, table_name, schema="", hidden=False ):
        """
        Parameter:
        ----------
        table_name : name of the table or view
        schema : schema containing the desired table
        hidden: : bool if True show hidden column (see https://www.sqlite.org/pragma.html#pragma_table_xinfo)

        Notes:
        ------
        This pragma returns one row for each normal column in the named table. Columns in the result set include:
        "name" (its name); "type" (data type if given, else ''); "notnull" (whether or not the column can be
        NULL); "dflt_value" (the default value for the column); and "pk" (either zero for columns that are not
        part of the primary key, or the 1-based index of the column within the primary key).
        https://www.sqlite.org/pragma.html#pragma_table_info 
        """
        if schema: schema = f"'{schema}'."
        if hidden: return self.table_xinfo( table_name, schema )
        
        sql = f"PRAGMA {schema}table_{hidden}info('{table_name}')"
        self.exe( sql )
        return self.fetch()


    def table_xinfo( self, table_name, schema="" ):
        """
        Notes:
        ------
        This pragma returns one row for each column in the named table, including generated columns
        and hidden columns. The output has the same columns as for PRAGMA table_info plus a column,
        "hidden", whose value signifies a normal column (0), a dynamic or stored generated column
        (2 or 3), or a hidden column in a virtual table (1). The rows for which this field is
        non-zero are those omitted for PRAGMA table_info.
        """
        if schema: schema = f"'{schema}'."
        sql = f"PRAGMA {schema}table_xinfo('{table_name}')"
        self.exe( sql )
        return self.fetch()


    def shrink_memory( self ):
        """
        Notes:
        ------
        This pragma causes the database connection on which it is invoked to free up as much memory as it can,
        by calling sqlite3_db_release_memory().
        """
        self.exe( f"PRAGMA shrink_memory" )

    
    def wal_checkpoint( self, mode="" ):
        """
        Notes:
        ------
        see documentation: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
        """
        if mode:    mode=f"({mode})"
        self.exe( f"PRAGMA wal_checkpoint{mode}" )


    ### some helper functions (static and classmethods)
    
    @staticmethod
    def sql_value_list( params, update=False ):
        """
        Parameter:
        ----------
        params : dict from which we want to create a proper SQL value list 
        update : bool make it compatible for an UPDATE clause

        Notes:
        ------
        TODO

        Return:
        -------
        complete VALUES statement
        """
        value_list = ""
        for i in params:
            if update:
                value_list += f'"{i}" = '
            if params[i] in gv.null_vars:
                value_list += "NULL, "
            else: value_list += f'"{params[i]}", '
        return value_list[:-2]

    
    @classmethod
    def sql_values( cls, params ):
        """
        Parameter:
        ----------
        params : dict from which we want to create a SQL string

        Return:
        -------
        single VALUES string
        """
        if len(params) == 1:
            return f"({list(params)[0]}) VALUES ({list(params.values())[0]})"
        else:
            param_keys = '", "'.join(params.keys())
            column_list = '"' + param_keys + '"'
            value_list  = cls.sql_value_list(params)

            return f"({column_list}) VALUES ({value_list})"
    
    
    # helper function which adds IN() around a the values OR produces a REGEXP statement (for ".")
    @staticmethod
    def sql_in(what, regexp=False):
        """
        """
        if regexp:      return "REGEXP '(" + "|".join(what) + ")'"
        else:           return "IN('"+"','".join(what)+"')"
    
    
    @classmethod
    def sql_equal_or_in(cls, what, regexp=False, like=False):
        """
        """
        l = len(what)
        if l == 1:
            what = what[0]
            if regexp:
                sql = f"REGEXP '{what}'"
            elif like:
                sql = f"LIKE '{what}'"
            else:
                sql = f"= '{what}'"
            return sql
        elif l > 1:
            return cls.sql_in(what, regexp=regexp)
    
    
    @staticmethod
    def fix_table_name( table ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if "." not in table: return f"'{table}'"
        return table

    @staticmethod
    def fix_column_name( column ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        # if the column name starts with a number we have to 'quote' it in SQLite...
        try:    test = int(column[0])
        except: return column
        return f"'{column}'"


    # SQL command functions like insert, select (distinct) and update (TODO)

    def insert( self, table, params, replace="", conflict = False, update = "", skip_update = (), verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose
        table = self.fix_table_name(table)

        #https://www.sqlite.org/lang_insert.html 
        values = self.sql_values(params)

        if replace: replace = "OR REPLACE "

        sql = f"INSERT {replace}INTO {table} " + values
        if conflict:
            print("CONFLICT")
            if type(conflict) == bool: # -> conflict == True
                conflict = ""
            elif len(conflict) > 1: # list of conflict columns
                conflict = " (" + ",".join(conflict) + ")"
            else: conflict = conflict[0]
            if type(update) == str:
                if len(update) == 0: update = "NOTHING"
                else: update = "UPDATE SET " + update
            elif type(update) == dict:
                update = "UPDATE SET " + self.sql_value_list(update, True)
            elif skip_update:
                for i in skip_update:
                    try: params.pop(i)
                    except: pass
                update = "UPDATE SET " + self.sql_value_list(params, True)
            else: update = f"UPDATE SET {update}"

            sql += f" ON CONFLICT{conflict} DO " + update

        try: self.exe( sql )
        except Exception as e:
            if traceback: print_trace(e)
            if verbose: print(f"INSERT command '{sql}' failed!")
            return False
        return True


    def select( self, column, table, where=None, what=None, distinct="", order=None, limit=None, fetchall=True ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        #https://www.sqlite.org/lang_select.html
       
        column = self.fix_column_name(column)
        table = self.fix_table_name(table)

        if distinct: distinct = "DISTINCT "
        sql = f"SELECT {distinct}{column} FROM {table} "
        if what and r"%" in what: operator = "LIKE"
        else: operator = "="

        if where:
            if type(where) in gv.array_types and len(where) > 1 and len(where) == len(what):
                s = "WHERE "
                for i in range(len(where)):
                    if type(what[i]) in gv.array_types:
                        regexp = False
                        for j in what[i]:
                            if "." in j: regexp = True
                        what_i = self.sql_in( what[i], regexp )
                    else: what_i = f"{operator} '{what[i]}'"
                    s += f"{where[i]} {what_i} AND "
                sql += s[:-5]
            else:
                if type(what) in gv.array_types:
                    regexp = False
                    for i in what:
                        if "." in i: regexp = True
                    what = self.sql_in( what, regexp )
                else: what = f"{operator} '{what}'"
                sql += f"WHERE {where} {what}"

        if order: sql += f"ORDER BY '{order}"
        if limit: sql += f"LIMIT {limit}"
        
        self.exe( sql )
        
        if fetchall:
            data = self.fetch()
            if data: return (_ for _ in data)
            else:    return ()
        else: return self.fetch1()


    def select_distinct( self, column, table, where=None, what=None, order=None, limit=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        return set(self.select( column, table, where=where, what=what, distinct=True, order=order, limit=limit ))

    
    #TODO update (UPDATE ... SET ... = ... WHERE


    def add_column( self, table, column, verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """        
        if verbose is None: verbose = self.verbose

        table = self.fix_table_name(table)
        column = self.fix_column_name(column)

        try: self.exe(f"ALTER TABLE {table} ADD COLUMN {column}")
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            if verbose: print("Column '{column}' already exist in table '{table}'.")
            return False
        return True
        
        
    def drop_column( self, table, column, verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        table = self.fix_table_name(table)
        column = self.fix_column_name(column)

        try: self.exe(f"ALTER TABLE {table} DROP COLUMN {column}")
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            if verbose: print("Column '{column}' does not exist in table '{table}'.")
            return False
        return True

    #TODO add more ALTER TABLE commands https://www.sqlite.org/lang_altertable.html
    
    def create_table( self, table, columns, exists="IF NOT EXISTS", verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        table = self.fix_table_name(table)

        if not exists: exists = ""
        sql = f"CREATE TABLE {exists} {table} ("
        
        if type(columns) == str: sql += columns
        
        elif type(columns) == dict:
            for i in columns:
                sql += i + " " + columns[i] + ", "
            sql = sql[:-2]
        sql += ")"
        
        try: self.exe( sql )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        return True


    def drop_table( self, table, exists="IF EXISTS", verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        table = self.fix_table_name(table)

        if not exists: exists = ""

        sql = f"DROP TABLE {exists} {table}"
        try: self.exe( sql )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        return True
    
    
    def count_tables(self):
        """
        Notes:
        ------
        count the amount of tables in current database and return it
        
        Return:
        -------
        number of tables as int
        """
        self.exe(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        return int( self.fetch1() )
        
    #TODO add CREATE INDEX statement https://www.sqlite.org/lang_createindex.html


    #TODO from here on move to getter_settter.py as soon as it uses the database class?


    def register_file( self, file_name, file_dir, source, status="locked", creation_date="NULL", addition_date=dt.utcnow(), verbose=None, traceback=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose
        if traceback is None: traceback = self.traceback

        values = f"VALUES ('{file_name}','{file_dir}','{source}','{status}','{creation_date}','{addition_date}')"
        sql = f"INSERT INTO file_table (name,dir,source,status,created,added) {values} ON CONFLICT DO NOTHING"
        try: self.exe( sql )
        except Exception as e:
            if traceback: gf.print_trace(e)
            return False
        return self.cur.lastrowid

    
    def register_files(self, names, dirs, sources, statuses, dates, verbose=None):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose
        
        N = len(names)
        
        if type(dirs) == str:
            dirs = [dirs] * N
        if type(sources) == str:
            sources = [source] * N
        if type(statuses) == str:
            statuses = [status] * N

        values = set()

        for i in range(N):
            values.add( (names[i], dirs[i], sources[i], statuses[i], dates[i]) )

        sql = f"INSERT INTO file_table (?,?,?,?,?) ON CONFLICT DO NOTHING"
        try: self.executemany( sql, values )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        return True


    def file_exists( self, file_name, file_dir ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT COUNT(*) FROM file_table WHERE name = '{file_name}' AND dir = '{file_dir}'"
        self.exe( sql )
        return self.fetch1()


    def get_file_id( self, file_name, file_dir ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT rowid FROM file_table WHERE name = '{file_name}' AND dir = '{file_dir}'"
        self.exe( sql )
        return self.fetch1()


    def get_file_X( self, ID, X ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT {X} FROM file_table WHERE rowid = '{ID}'"
        self.exe( sql )
        return self.fetch1()


    get_file_name   = lambda self, ID : self.get_file_X( ID, "name" )
    get_file_dir    = lambda self, ID : self.get_file_X( ID, "dir" )
    get_file_date   = lambda self, ID : self.get_file_X( ID, "date" )
    get_file_source = lambda self, ID : self.get_file_X( ID, "source" )
    get_file_status = lambda self, ID : self.get_file_X( ID, "status" )


    def set_file_X( self, ID, X, what ):
        """
        Parameter:
        ----------
        ID : file ID, unique int associated to file in datbase (stored in table_files)
        X : file property we want to set
        what: value we want to set X from the file in database

        Return:
        -------
        True if succesful, False if not
        """
        sql = f"UPDATE file_table SET '{X}' = '{what}' WHERE rowid = {ID}"
        try: self.exe( sql )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        return True


    set_file_name   = lambda self, ID, file_name    : self.set_file_X( ID, "name", file_name )
    set_file_dir    = lambda self, ID, file_dir     : self.set_file_X( ID, "dir", file_dir )
    set_file_source = lambda self, ID, source       : self.set_file_X( ID, "source", source )
    

    def set_file_date( self, ID, date, verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        if verbose:
            name = self.get_file_name( ID )
            print(f"Setting date of FILE '{name}' to '{date}'")
        
        sql = f"UPDATE file_table SET date = '{date}' WHERE rowid = '{ID}'"

        try: self.exe( sql )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        return True


    def set_file_status( self, ID, status, verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        if status != "parsed" and verbose:
            name = self.get_file_name( ID )
            print(f"Setting status of FILE '{name}' with ID '{ID}' to '{status}'")
        sql = f"UPDATE file_table SET status = '{status}' WHERE rowid = '{ID}'"
        try: self.exe( sql )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        return True

    
    def set_file_statuses( self, file_statuses, retries=100, timeout=5, verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        sql = "UPDATE file_table SET status = ? WHERE rowid = ?"

        while retries > 0:
            try: self.exemany( sql, file_statuses )
            except sqlite3.OperationalError as e:
                message = f"Failed to update file statuses! Database locked?"
                if verbose: print(message)
                retries -= 1
            else: break


    def get_files_with_status( self, status, source=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if source:
            return self.select_distinct( "name", "file_table", ("status","source"), (status, source) )
        else:
            return self.select_distinct( "name", "file_table", "status", status )


    def get_stations( self, cluster=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if not cluster: return self.select_distinct( "location", "station_table" )
        return self.select_distinct( "location", "station_table", "cluster", cluster )


    def get_station_info(self, location ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT ICAO,name,longitude,latitude,elevation,cluster,orography FROM station_table WHERE location={location}"
        from sql_factories import dict_row, default
        self.con.row_factory = dict_row
        self.exe(sql)
        # reset the row_factory function to default TODO is this really necessary?
        self.con.row_factory = default
        return self.fetch()


    def get_station_X(self, location, X):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT {X} FROM station_table WHERE location = '{location}'"
        self.exe( sql )
        return self.fetch1()


    get_station_icao        = lambda self, location : str( self.get_station_X(location, "ICAO") )
    get_station_name        = lambda self, location : str( self.get_station_X(location, "name") )
    get_station_longitude   = lambda self, location : float( self.get_station_X(location, "longitude") )
    get_station_latitude    = lambda self, location : float( self.get_station_X(location, "latitude") )
    get_station_elevation   = lambda self, location : float( self.get_station_X(location, "elevation") )
    get_station_baro_elev   = lambda self, location : float( self.get_station_X(location, "baro_elevation") )
    get_station_cluster     = lambda self, location : str( self.get_station_X(location, "cluster") )
    get_station_orography   = lambda self, location : str( self.get_station_X(location, "orography") )


    def get_station_location( self, name=None, icao=None, cluster=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------
        get station location (wmo ID) by name or ICAO
        if name is given, additionally cluster can be provided

        Return:
        -------

        """
        sql = "SELECT location FROM station_table WHERE "
        if name:
            sql += f"name='{name}'"
            if cluster: sql += " AND cluster = '{cluster}'"
        elif icao:  sql += f"ICAO='{icao}'"
        self.exe(sql)
        return self.fetch1(sql)


    def station_exists( self, location ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = f"SELECT COUNT(*) FROM station_table WHERE location = '{location}'"
        self.exe( sql )
        return self.fetch1()


    def add_station( self, station_data, update=False, commit=True, verbose=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if verbose is None: verbose = self.verbose

        location = station_data[0]
        if self.station_exists( location ) and update == False:
            if verbose: print("Station", location, "already exists in database.")
            return
        if verbose: print("Adding", location, "to database...")

        if update:
            print("Updating station", location, "...")
            sql = f"""UPDATE station_table SET location='?', ICAO='?', name='?', longitude=?, latitude=?,
            elevation=?, cluster=?, orography=? WHERE location = {location}"""
        else:
            sql = f"INSERT INTO station_table VALUES(?,?,?,?,?,?,?,?)"

        try: self.exe( sql, station_data )
        except Exception as e:
            if self.traceback: gf.print_trace(e)
            return False
        if commit: self.commit()
        return True


    def get_elements( self, path_identifier=None ):
        #TODO
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        sql = "SELECT DISTINCT element FROM element_table WHERE role='obs'"
        if path_identifier:
            sql += f" AND path_identifier LIKE '%{path_identifier}%'"
        self.exe(sql)
        return set(self.fetch())
