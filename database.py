from functions import read_yaml, read_file

class db:

    def __init__(self):
        #read config.yaml to dictionary
        config_yaml      = read_yaml( "config.yaml" )
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

    def sql_insert(self, table, params, conflict = None, skip_update = () ):
        sql = f"INSERT INTO {table} " + self.sql_values(params)
        if conflict:
            for i in skip_update:
                try:    params.pop(i)
                except: continue
            sql += f" ON CONFLICT({conflict}) DO UPDATE SET " + self.sql_value_list(params,True)
        return sql

    def select_distinct( self, column, table, where=None, what=None ):
        sql = f"SELECT DISTINCT {column} FROM {table} "
        if where:
            if type(what) == tuple: what = "IN('"+"','".join(what)+"')"
            else: what = f"= '{what}'"
            sql += f"WHERE {where} {what}"
        self.cur.execute( sql )
        data = self.cur.fetchall()
        if data: return set(i[0] for i in data)
        else:    return set()

    def register_file( self, name, path, source, status="locked" ):
        values = f"VALUES ('{name}','{path}','{source}','{status}')"
        sql    = f"INSERT INTO files (name,path,source,status) {values}"
        self.cur.execute( sql )
        return self.cur.lastrowid

    def get_file_status( self, name, source ):
        sql = f"SELECT status FROM files WHERE name = '{name}' AND source = '{source}'"
        self.cur.execute( sql )
        status = self.cur.fetchone()
        if status: return status
        else:      return None

    def set_file_status( self, name, status, verbose=False ):
        sql = f"UPDATE files SET status = '{status}'"
        self.cur.execute( sql )
        if status != "parsed":
            if verbose: print(f"Setting status of FILE '{name}' to '{status}'")

    known_stations  = lambda self         : self.select_distinct( "stID", "station" )
    files_status    = lambda self, status : self.select_distinct( "name", "files", "status", status )
