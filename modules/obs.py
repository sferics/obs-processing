import sqlite3
from copy import copy
from database import DatabaseClass
from config import ConfigClass
import global_functions as gf
import global_variables as gv


class ObsClass:
    @classmethod
    def __init__(self, cf: ConfigClass, source: str="extra", mode: str="dev", stage: str="raw",
            verbose: bool=False):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------
        
        """
        assert( mode in {"dev", "oper"} )
        assert( stage in {"raw", "forge", "final"} )
        
        # in this merge we are adding only already present keys; while again overwriting them
        config      = gf.merge_list_of_dicts([cf.general, cf.obs, cf.script], add_keys=True)
        # make config and important definitions accessible as class objects
        self.config = config
        self.stage  = stage
        
        for key, val in config.items():
            if verbose: print(key, val)
            setattr(self, key, val)
        
        self.source = source

        self.log = gf.get_logger( self.__class__.__name__, self.log_level )
        if verbose: print()

     
    @classmethod
    def get_station_db_dir(self, loc, output=None, mode=None, stage=None):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if output is None:  output  = self.output
        if mode is None:    mode    = self.mode
        if stage is None:   stage   = self.stage

        return f"{output}/{mode}/{stage}/{loc[0]}"
    
    @classmethod
    def create_station_db_dir(self, loc, output=None, mode=None, stage=None):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if output is None:  output  = self.output
        if mode is None:    mode    = self.mode
        if stage is None:   stage   = self.stage

        station_db_dir = self.get_station_db_dir(loc, output, mode, stage)
        gf.create_dir( station_db_dir )
        return station_db_dir

    @classmethod
    def get_station_db_path(self, loc, output=None, mode=None, stage=None):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if output is None:  output  = self.output
        if mode is None:    mode    = self.mode
        if stage is None:   stage   = self.stage
        
        return self.get_station_db_dir(loc, output, mode, stage) + f"/{loc}.db"


    def to_station_databases(self, obs_db, source=None, scale=None, prio=0, mode=None, stage=None,
            output=None, max_retries=None, commit=None, update=None, timeout=None, traceback=None,
            verbose=None, settings={}):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        if source is None:      source      = self.source
        if output is None:      output      = self.output
        if mode is None:        mode        = self.mode
        if stage is None:       stage       = self.stage
        if max_retries is None: max_retries = self.max_retries
        if timeout is None:     timeout     = self.timeout
        if traceback is None:   tracback    = self.traceback
        if verbose is None:     verbose     = self.verbose
        if settings is {}:      settings    = self.settings
        
        # insert values or update value if we have a newer cor, then set parsed = 0 as well
        # statements for different stages
        match stage:
            case "raw":
                if scale:
                    sql = (f"INSERT INTO obs (dataset,file,datetime,duration,element,value,cor,"
                        f"scale,prio) VALUES ('{source}',?,?,?,?,?,?,?,{prio}) ON CONFLICT DO " )
                    if update:
                        sql += ("UPDATE SET value=excluded.value, file=excluded.file, "
                            "cor=excluded.cor, reduced=0 WHERE excluded.cor > obs.cor" )
                            # AND excluded.file > obs.file
                        if mode == "dev": sql += " AND excluded.scale > obs.scale"
                    else: sql += "NOTHING"
                else:
                    sql = ( f"INSERT INTO obs (dataset,file,datetime,duration,element,value,cor,prio) "
                            f"VALUES ('{source}',?,?,?,?,?,?,{prio}) ON CONFLICT DO " )
                    if update:
                        sql += ("UPDATE SET value=excluded.value, file=excluded.file, cor=excluded.cor, "
                            "reduced=0 WHERE excluded.cor > obs.cor" ) # AND excluded.file > obs.file
                    else: sql += "NOTHING"
            case "forge":
                sql = ("INSERT INTO obs (dataset,datetime,duration,value) VALUES(?,?,?,?) "
                    "ON CONFLICT DO ")
                if update:
                    sql += "UPDATE SET duration=excluded.duration, value=excluded.value"
                else: sql += "NOTHING"
            case "final":
                sql = f"INSERT INTO obs (dataset,datetime,value) VALUES(?,?,?)"
                if update:
                    sql += "UPDATE SET value=excluded.value"
                else: sql += "NOTHING"
        
        for loc in obs_db:
            created = self.create_station_tables(loc, output, mode, stage, max_retries, 1, 1)
            if not created: continue
            
            retries = copy(max_retries)
            config_dict = {"timeout":timeout, "traceback":traceback, "settings":settings, "verbose":verbose}
            
            while retries > 0:
                try:
                    station_db_path = self.get_station_db_path(loc, output, mode, stage)
                    db_loc = DatabaseClass( station_db_path, config_dict )
                    db_loc.exemany( sql, obs_db[loc] )
                except sqlite3.Error as e:
                    print(e, retries)
                    retries -= 1
                    if verbose: print(f"Retrying to insert data", retries, "times")
                    continue
                else:
                    #TODO implement stage match case for SQL and try/except/else for other stages
                    if stage == "raw" and verbose:
                        print(loc)
                        loc = list(obs_db[loc])
                        for i in range(len(loc)):
                            # try to print CCX if observationSequenceNumber is available 
                            try:
                                if loc[i][5]:   cor = "CC" + chr(64+loc[i][5])
                                else:           cor = ""
                            except:             cor = ""
                            print(f"{loc[i][1]} {loc[i][2]:<6} {loc[i][3]:<20} {loc[i][4]:<21} {cor}")
                        print()
                    break
            
            if retries == 0:
                return False
            else:
                db_loc.close(commit=True)


    def create_station_tables( self, loc, output=None, mode=None, stage=None, max_retries=None, commit=None, timeout=None, traceback=None, verbose=None, settings={} ):
        """
        Parameter:
        ----------
        loc : station location, usually WMO ID
        output: where the station databases are saved
        commit : commit to database afterwards
        verbose : print exceptions that are being raised

        Notes:
        ------
        creates the obs,model and stats tables for a new station

        Return:
        -------
        True if succesful, False if not, None if tables already exists and completely setup (ready == 1)
        """
        if output is None:      output      = self.output
        if mode is None:        mode        = self.mode
        if stage is None:       stage       = self.stage
        if max_retries is None: max_retries = self.max_retries
        if commit is None:      commit      = self.commit
        if timeout is None:     timeout     = self.timeout
        if traceback is None:   tracback    = self.traceback
        if verbose is None:     verbose     = self.verbose
        if settings is {}:      settings    = self.settings

        station_db_dir  = self.create_station_db_dir(loc, output, mode, stage)
        station_db_path = station_db_dir + f"/{loc}.db"
        
        ready = False

        retries = copy(max_retries)

        while retries > 0:
            try:
                db_loc = DatabaseClass( station_db_path, {"timeout":timeout, "traceback":traceback,
                    "settings":settings, "verbose":verbose} )
                # get number of tables in attached station DB
                tables_counted = db_loc.count_tables()
            except sqlite3.Error as e:
                print(e, retries)
                retries -= 1
                continue
            else:
                # find out if the right amount of tables has been successfully created
                match stage:
                    case "raw" | "forge":
                        match mode:
                            case "dev" | "oper":    tables = 1
                            case "test":            tables = 2
                    case "final":
                        match mode:
                            case "dev":             tables = 4
                            case "oper":            tables = 3
                            case "test":            tables = 5
                
                # these values are hardcoded for performance reasons, remember to change them!
                ready = ( tables_counted == tables )
                break

        if retries == 0: return False

        if ready:
            db_loc.close()
            return True
        else:
            if verbose: print("Creating table and adding columns...")

            # read yaml structure file for station tables into a dict
            tables = gf.read_yaml( f"station_tables/{mode}_{stage}", file_dir=self.config_dir )

            for table in tables:
                retries = copy(max_retries)
                while retries > 0:
                    try:
                        created = db_loc.create_table( table, tables[table], verbose=verbose )
                    except sqlite.Error as e:
                        print(e)
                        retries -= 1
                        continue
                    else:
                        if not created: retries -= 1; continue
                        else: break

                if retries == 0: return False

        db_loc.close(commit=commit)
        return True
