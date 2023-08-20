from copy import copy
from database import database
import global_functions as gf
import global_variables as gv


class obs:
    def __init__(self, source, config={"output_path":"", "max_retries":100, "commit":True, "timeout":5, "log_level":"DEBUG", "traceback":True, "verbose":False, "settings":{} }, typ="raw"):
        
        assert( type(config) == dict and config["output_path"] )
        
        self.source         = source
        self.typ            = typ
        self.output_path    = config["output_path"]
        self.max_retries    = config["max_retries"]
        self.commit         = config["commit"]
        self.timeout        = config["timeout"]
        self.log_level      = config["log_level"]
        self.traceback      = config["traceback"]
        self.verbose        = config["verbose"]
        self.settings       = config["settings"]

        assert(config["log_level"] in gv.log_levels)


    #TODO add update=bool flag (ON CONFLICT DO UPDATE clause on/off)
    def to_station_databases( obs_db, source, typ=None, output_path=None, max_retries=None, commit=None, timeout=None, traceback=None, verbose=None, settings=None ):
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
        sql = ( f"INSERT INTO obs (dataset,file,datetime,duration,element,value,cor) VALUES "
            f"('{source}',?,?,?,?,?,?) ON CONFLICT DO UPDATE SET value = excluded.value, reduced = 0, "
            f"file = excluded.file WHERE excluded.cor > obs.cor" )

        for loc in obs_db:
            created = self.create_station_tables(loc, output_path, "raw", max_retries, 1, 1, verbose=verbose)
            if not created: continue

            retries = copy(max_retries)

            while retries > 0:
                try:
                    db_loc = database( f"{output_path}/raw/{loc[0]}/{loc}.db", timeout=timeout, traceback=traceback, settings=settings, verbose=verbose)
                    db_loc.exemany( sql, obs_db[loc] )
                except sqlite3.Error as e:
                    print(e, retries)
                    retries -= 1
                    if verbose: print(f"Retrying to insert data", retries, "times")
                    continue
                else:
                    if verbose:
                        print(loc)
                        loc = list(obs_db[loc])
                        for i in range(len(loc)):
                            if loc[i][5]:   cor = "CC" + chr(64+loc[i][5])
                            else:           cor = ""
                            print(f"{loc[i][1]} {loc[i][2]:<6} {loc[i][3]:<20} {loc[i][4]:<21} {cor}")
                        print()
                    break

            db_loc.close(commit=True)


    def create_station_tables( loc, output_path=None, typ=None, max_retries=None, commit=None, timeout=None, traceback=None, verbose=None, settings=None ):
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
        create_dir( station_path )
        db_path = f'{station_path}/{loc}.db'

        ready = False

        retries = copy(max_retries)

        while retries > 0:
            try:
                db_loc = database( db_path, timeout=timeout, traceback=traceback, settings=settings, verbose=verbose )
                # get number of tables in attached DB
                db_loc.exe(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                n_tables = db.fetch1()
            except sqlite3.Error as e:
                print(e, retries)
                retries -= 1
                continue
            else:
                ready = ( n_tables == 3 ) # 3 is hardcoded for performance reasons, remember to change!
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
