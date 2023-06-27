from functions import read_yaml, read_file

class db:

    def __init__(self, config="config.yaml"):
        #read config.yaml to dictionary
        config_yaml      = read_yaml( config )
        self.config      = config_yaml["database"]
        self.name        = self.config["name"]
        self.tables      = self.config["tables"]
        self.table_ext   = self.config["table_ext"]
        self.null_vals   = self.config["null_vals"]

        #set up database cursor and cursor
        from sqlite3 import connect  #python sqlite connector
        self.con    = connect(self.name)  # Creating obs db and opening database connection
        self.cur    = self.con.cursor()   # Creating cursor object to call SQL
        self.commit = lambda : self.con.commit()

        #read sql files to create db tables and execute the SQL statements
        for table in self.tables:
            self.cur.execute( read_file( table + "." + self.table_ext ) )

    def close(self):
        self.commit()
        self.cur.close()
        self.con.close()
    
    def sql_value_list( self, params, update=False ):
        value_list = ""
        for i in params:
            if update:
                value_list += f'"{i}" = '
            if params[i] in self.null_vals:
                value_list += "NULL, "
            else:
                value_list += f'"{params[i]}", '
        return value_list[:-2]

    def sql_values( self, params ):
        column_list = '"' + '", "'.join(params.keys()) + '"'
        value_list  = self.sql_value_list(params)
        return f"({column_list}) VALUES ({value_list})"

    def sql_insert(self, table, params, conflict = (), skip_update = () ):
        
        sql = f"INSERT INTO {table} " + self.sql_values(params)
        if len(conflict) > 0:
            if len(conflict) > 1:
                conflict = ",".join(conflict)
            else: conflict = conflict[0]
            for i in skip_update:
                try:    params.pop(i)
                except: continue
            sql += f" ON CONFLICT({conflict}) DO UPDATE SET " + self.sql_value_list(params,True)
        
        self.cur.execute( sql )

    sql_in = lambda self, what : "IN('"+"','".join(what)+"')"

    def select_distinct( self, column, table, where=None, what=None ):
        sql = f"SELECT DISTINCT {column} FROM {table} "
        if where:
            if type(where) == tuple and len(where) > 1 and len(where) == len(what):
                s = "WHERE "
                for i in range(len(where)):
                    if type(what[i]) == tuple: what_i = self.sql_in( what[i] )
                    else: what_i = f"= '{what[i]}'"
                    s += f"{where[i]} {what_i} AND "
                sql += s[:-5]
            else:  
                if type(what) == tuple: what = self.sql_in( what )
                else: what = f"= '{what}'"
                sql += f"WHERE {where} {what}"
        self.cur.execute( sql )
        data = self.cur.fetchall()
        if data: return set(i[0] for i in data)
        else:    return set()

    def add_column( self, table, column, verbose=False ):
        try: self.cur.execute(f'ALTER TABLE {table} ADD COLUMN "{column}"')
        except Exception as e:
            if verbose:
                print(e)
                print("Column '{column}' already exist in table '{table}'!")

    def drop_column( self, table, column, verbose=False ):
        try: self.cur.execute(f'ALTER TABLE {table} DROP COLUMN "{column}"')
        except Exception as e:
            if verbose:
                print(e)
                print("Column '{column}' does not exist in table '{table}'!")

    def register_file( self, name, path, source, status="locked", date="NULL", verbose=False ):
        values = f"VALUES ('{name}','{path}','{source}','{status}','{date}')"
        sql    = f"INSERT INTO files (name,path,source,status,date) {values}"
        try:
            self.cur.execute( sql )
            return self.cur.lastrowid
        except Exception as e:
            if verbose: print(e)

    def file_exists( self, name, path ):
        sql = f"SELECT COUNT(*) FROM files WHERE name = '{name}' AND path = '{path}'"
        self.cur.execute( sql )
        try:    return self.cur.fetchone()[0]
        except: return None

    def get_file_id( self, name, path ):
        sql = f"SELECT rowid FROM files WHERE name = '{name}' AND path = '{path}'"
        self.cur.execute( sql )
        try:    return self.cur.fetchone()[0]
        except: return None

    def get_file_name( self, ID ):
        sql = f"SELECT name FROM files WHERE rowid = '{ID}'"
        self.cur.execute( sql )
        try:    return self.cur.fetchone()[0]
        except: return None

    def get_file_path( self, ID ):
        sql = f"SELECT path FROM files WHERE name = '{ID}'"
        self.cur.execute( sql )
        try:    return self.cur.fetchone()[0]
        except: return None

    def get_file_source( self, ID ):
        sql = f"SELECT source FROM files WHERE name = '{ID}'"
        self.cur.execute( sql )
        try:    return self.cur.fetchone()[0]
        except: return None

    def get_file_status( self, ID ):
        sql = f"SELECT status FROM files WHERE rowid = '{ID}'"
        self.cur.execute( sql )
        try:    return self.cur.fetchone()[0]
        except: return None

    def set_file_status( self, ID, status, verbose=False ):
        if status != "parsed" and verbose:
            name = self.get_file_name( ID )
            print(f"Setting status of FILE '{name}' to '{status}'")
        sql = f"UPDATE files SET status = '{status}' WHERE rowid = '{ID}'"
        try: self.cur.execute( sql )
        except Exception as e:
            if verbose: print(e)

    def get_file_date( self, ID ):
        sql = f"SELECT date FROM files WHERE rowid = '{ID}'"
        self.cur.execute( sql )
        date = self.cur.fetchone()[0]
        if date: return date
        else:    return None

    def set_file_date( self, ID, date, verbose=False ):
        if verbose:
            name = self.get_file_name( ID )
            print(f"Setting date of FILE '{name}' to '{date}'")
        
        try:
            sql = f"UPDATE files SET date = '{date}' WHERE rowid = '{ID}'"
            self.cur.execute( sql )
        except Exception as e:
            if verbose: print(e)

    def files_status( self, status, source=None ):
        if source:
            return self.select_distinct( "name", "files", ("status", "source"), (status, source) )
        else:
            return self.select_distinct( "name", "files", "status", status )

    known_stations  = lambda self : self.select_distinct( "stID", "station" )
