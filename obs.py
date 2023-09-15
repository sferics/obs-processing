import sqlite3
from copy import copy
from database import database_class
import global_functions as gf
import global_variables as gv


class obs_class:
    def __init__(self, typ: str="raw", config: dict={}, source: str="test", mode: str="dev") -> object:
        """
        """
        #TODO can we remove typ and replace its position by mode?
        # when would we ever want to insert obs into databases which are not RAW?
        
        assert( typ in {"raw","forge","dev","oper"} and mode in {"dev","oper","test"} )

        self.source = source
        self.typ    = typ
        self.mode   = mode
       
        try:    self.output_path    = str(config["output_path"])
        except: self.output_path    = "/home/juri/data/stations"

        try:    self.max_retries    = int(config["max_retries"])
        except: self.max_retries    = 100
        
        try:    self.commit         = bool(config["commit"])
        except: self.commit         = True
        
        try:    self.timeout        = float(config["timeout"])
        except: self.timeout        = 5
        
        try:    self.traceback      = bool(config["traceback"])
        except: self.traceback      = False
        
        try:    self.verbose        = bool(config["verbose"])
        except: self.verbose        = False
        
        try:    self.settings       = dict(config["settings"])
        except: self.settings       = {}
        
        if "log_level" in config and config["log_level"] in gv.log_levels:
            self.log_level = config["log_level"]
        else: self.log_level = "NOTSET"

        self.log = gf.get_logger( self.__class__.__name__, self.log_level )


    #TODO add update=bool flag (ON CONFLICT DO UPDATE clause on/off)
    def to_station_databases(self, obs_db, source=None, typ=None, output_path=None, max_retries=None, commit=None, timeout=None, traceback=None, verbose=None, settings=None):
        #TODO
        """
        """
        #TODO implement typ match case for SQL and try/except/else: if verbose part
        if source is None:      source      = self.source
        if output_path is None: output_path = self.output_path
        if typ is None:         typ         = self.typ
        if max_retries is None: max_retries = self.max_retries
        if commit is None:      commit      = self.commit
        if timeout is None:     timeout     = self.timeout
        if traceback is None:   tracback    = self.traceback
        if verbose is None:     verbose     = self.verbose
        if settings is None:    settings    = self.settings

        # insert values or update value if we have a newer cor, then set parsed = 0 as well
        #TODO statements for different typs
        match typ:
            case "raw":
                sql = ( f"INSERT INTO obs (dataset,file,datetime,duration,element,value,cor) VALUES "
                    f"('{source}',?,?,?,?,?,?) ON CONFLICT DO UPDATE SET value = excluded.value, reduced = 0, "
                    f"file = excluded.file WHERE excluded.cor > obs.cor and excluded.file > obs.file" )
            case "forge" | "dev" | "oper":
                sql = ( f"INSERT INTO obs (dataset,datetime,duration,value) VALUES(?,?,?,?) "
                        f"ON CONFLICT DO UPDATE SET value=excluded.value" )
        
        for loc in obs_db:
            created = self.create_station_tables(loc, output_path, typ, max_retries, 1, 1, verbose=verbose)
            if not created: continue

            retries = copy(max_retries)

            while retries > 0:
                try:
                    db_loc = database_class( f"{output_path}/{typ}/{loc[0]}/{loc}.db", {"timeout":timeout, "traceback":traceback, "settings":settings, "verbose":verbose} )
                    db_loc.exemany( sql, obs_db[loc] )
                except sqlite3.Error as e:
                    print(e, retries)
                    retries -= 1
                    if verbose: print(f"Retrying to insert data", retries, "times")
                    continue
                else:
                    if typ == "raw" and verbose:
                        print(loc)
                        loc = list(obs_db[loc])
                        for i in range(len(loc)):
                            
                            try:
                                if loc[i][5]:   cor = "CC" + chr(64+loc[i][5])
                                else:           cor = ""
                            except:             cor = ""
                            print(f"{loc[i][1]} {loc[i][2]:<6} {loc[i][3]:<20} {loc[i][4]:<21} {cor}")
                        print()
                    break

            db_loc.close(commit=True)


    def create_station_tables( self, loc, output_path=None, typ=None, max_retries=None, commit=None, timeout=None, traceback=None, verbose=None, settings=None ):
        """
        Parameter:
        ----------
        loc : station location, usually WMO ID
        output_path : where the station databases are saved
        commit : commit to database afterwards
        verbose : print exceptions that are being raised

        Notes:
        ------
        creates the obs,model and stats tables for a new station

        Return:
        -------
        True if succesful, False if not, None if tables already exists and completely setup (ready == 1)
        """
        if output_path is None: output_path = self.output_path
        if typ is None:         typ         = self.typ
        if max_retries is None: max_retries = self.max_retries
        if commit is None:      commit      = self.commit
        if timeout is None:     timeout     = self.timeout
        if traceback is None:   tracback    = self.traceback
        if verbose is None:     verbose     = self.verbose
        if settings is None:    settings    = self.settings

        station_path = f'{output_path}/{typ}/{loc[0]}'
        gf.create_dir( station_path )
        db_path = f'{station_path}/{loc}.db'

        ready = False

        retries = copy(max_retries)

        while retries > 0:
            try:
                db_loc = database_class( db_path, {"timeout":timeout, "traceback":traceback, "settings":settings, "verbose":verbose} )
                # get number of tables in attached DB
                db_loc.exe(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                n_tables = db_loc.fetch1()
            except sqlite3.Error as e:
                print(e, retries)
                retries -= 1
                continue
            else:
                match typ:
                    case "raw" | "dev":     typ_tables = 3
                    case "forge":           typ_tables = 1
                    case "oper":            typ_tables = 2
                ready = ( n_tables == typ_tables ) # is hardcoded for performance reasons, remember to change!
                break

        if retries == 0: return False

        if ready:
            db_loc.close()
            return True
        else:
            if verbose: print("Creating table and adding columns...")

            # read structure file station_tables.yaml into a dict
            tables = gf.read_yaml( f"station_tables_{typ}.yaml" )

            for table in tables:
                retries = copy(max_retries)
                while retries > 0:
                    try:
                        created = db_loc.create_table( table, tables[table], verbose=verbose )
                    except sqlite.Error as e:
                        print(e, retries)
                        retries -= 1
                        continue
                    else:
                        if not created: retries -= 1; continue
                        else: break

                if retries == 0: return False

        db_loc.close(commit=commit)
        return True
