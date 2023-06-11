#/!venv/bin/python3
#decode bufr and save obs to database

from glob import glob        #file lookup
import eccodes as ec         #bufr decoder by ECMWF
from sqlite3 import connect  #python sqlite connector
import re,sys,os,yaml,psutil #regular expressions, system, operating system and YAML config handling
from pathlib import Path     #path operation
from datetime import datetime as dt


clear      = lambda keyname           : str( re.sub( r"#[0-9]+#", '', keyname ) )
number     = lambda keyname           : int( re.sub( r"#[A-Za-z0-9]+", "", keyname[1:]) )
get_bufr   = lambda bufr, number, key : ec.codes_get( bufr, f"#{number}#{key}" )

def sql_value_list(params, update=False):
    value_list = ""
    for i in params:
        if update:                 value_list += f'"{i}" = '
        if params[i] in null_vals: value_list += "NULL, "
        else:                      value_list += f'"{params[i]}", '
    return value_list[:-2]

def sql_values(params):
    column_list = '"' + '", "'.join(params.keys()) + '"'
    value_list  = sql_value_list(params)
    return f"({column_list}) VALUES ({value_list})"

def sql_insert(table, params, conflict = None, skip_update = () ):
    sql = f"INSERT INTO {table} " + sql_values(params)
    if conflict:
        for i in skip_update:
            try:    params.pop(i)
            except: continue
        sql += f" ON CONFLICT({conflict}) DO UPDATE SET " + sql_value_list(params,True)
    return sql

def select_distinct( column, table, where=None, what=None ):
    sql = f"SELECT DISTINCT {column} FROM {table} "
    if where:
        if type(what) == tuple: what = "IN('"+"','".join(what)+"')"
        else: what = f"= '{what}'"
        sql += f"WHERE {where} {what}"
    cur.execute( sql )
    data = cur.fetchall()
    if data: return set(i[0] for i in data)
    else:    return set()

def register_file( name, path, source, status="locked" ):
    values = f"VALUES ('{name}','{path}','{source}','{status}')"
    sql    = f"INSERT INTO files (name,path,source,status) {values}"
    cur.execute( sql )
    return cur.lastrowid

def get_file_status( name ):
    sql = f"SELECT status FROM files WHERE name = '{name}'"
    cur.execute( sql )
    status = cur.fetchone()
    if status: return status
    else:      return None

def set_file_status( name, status ):
    sql = f"UPDATE files SET status = '{status}'"
    cur.execute( sql )
    if status != "parsed":
        if verbose: print(f"Setting status of FILE '{name}' to '{status}'")

known_stations  = lambda : select_distinct( "stID", "station" )
files_status    = lambda status : select_distinct( "name", "files", "status", status )

read_file = lambda file_name : Path( file_name ).read_text()

def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.load(f, yaml.Loader)


#read config.yaml
config        = read_yaml( "config.yaml" )
db_config     = config["database"]
db_name       = db_config["name"]
db_tables     = db_config["tables"]
db_table_ext  = db_config["table_ext"]

#set up database cursor and cursor
db  = connect(db_name)  # Creating obs db and opening database connection
cur = db.cursor()       # Creating cursor object to call SQL

def close_db(db, cur):
    db.commit(); cur.close()
    db.close()

#read sql files to create db tables and execute the SQL statements
read_file = lambda file_name : Path( file_name ).read_text()
for table in db_tables: cur.execute( read_file( table + "." + db_table_ext ) )

priority      = config["priorities"]["bufr"]
station_info  = config["station_info"]

config_script = config["scripts"][sys.argv[0]]
null_vals     = set( config_script["null_vals"] + [None] )
time_keys     =  config_script["time_keys"]
skip_keys     = config_script["skip_keys"]
skip_status   = config_script["skip_status"]
multi_file    = config_script["multi_file"]

verbose       = config_script["verbose"]
profile       = config_script["profile"]
logging       = config_script["logging"]
if profile: import cProfiler
if logging: import logging

if len(sys.argv) == 2:
    source = config["sources"][sys.argv[1]]
    
    if "," in source:
        sources = source.split(","); config_sources = {}
        for s in sources: config_sources[s] = config["sources"][s]
    
    else: config_sources = { sys.argv[1] : config["sources"][sys.argv[1]] }
else: config_sources = config["sources"]


def parse_all_bufrs( source ):
   
    bufr_dir      = source + "/"
    config_source = config_sources[source]
    ext           = config_source["bufr"]["ext"]
    if type(ext) == list:
        ext = r"[" + "][".join(ext) + "]"
        print(ext)

    skip_files     = set(files_status( skip_status ))
    files_in_dir   = set((os.path.basename(i) for i in glob( bufr_dir + f"*.{ext}" )))
    files_to_parse = files_in_dir - skip_files

    if verbose:
        print("#FILES in DIR:  ", len(files_in_dir))
        print("#FILES in DB:   ", len(skip_files))
        print("#FILES to parse:", len(files_to_parse))

    Path(bufr_dir).mkdir(exist_ok=True)

    for FILE in files_to_parse:
       
        #if file status is 'locked' continue with next file
        if get_file_status( FILE ) == "locked": continue

        parsed_counter = 0
        skip_obs       = False
        station0       = False
        source_name    = source[:]

        if source == "DWD":
            if FILE[-29:-26] == "GER":  source_name += "_ger"
            else:                       source_name += "_int"
        
        file_path      = str( Path( bufr_dir + FILE ).resolve().parent )
        #set file status = locked and get rowid (FILE ID)
        ID = register_file( FILE, file_path, source_name )

        with open(bufr_dir + FILE, "rb") as f:
            try:
                bufr = ec.codes_bufr_new_from_file(f)
                if bufr is None:
                    set_file_status( FILE, "empty" )
                    continue
                ec.codes_set(bufr, "skipExtraKeyAttributes",  1)
                ec.codes_set(bufr, "unpack", 1)
                iterid = ec.codes_bufr_keys_iterator_new(bufr)
            except Exception as e:
                if verbose: print(e)
                set_file_status( FILE, "error" )
                continue
            
            keys, key0 = {}, False

            while ec.codes_bufr_keys_iterator_next(iterid):
                keyname   = ec.codes_bufr_keys_iterator_get_name(iterid)
                clear_key = clear(keyname)
                try: num = number(keyname)
                except: continue

                if "->" in keyname: continue #associated field
                elif "#" in keyname:
                    if clear_key not in list(skip_keys) + list(station_info):
                        try:    keys[number(keyname)].add( clear_key )
                        except: keys[number(keyname)] = set()
                elif not multi_file:
                    try:    keys[0].add( keyname )
                    except: keys[0] = set()

            if source not in multi_file: #workaround
                key0 = True
                #BUFR messages all valid for one single station
                obs, meta = { "file" : ID, "priority" : priority }, {}
                for si in station_info:
                    try:                   meta[si] = ec.codes_get( bufr, si )
                    except Exception as e: meta[si] = None
                try: del keys[0]
                except: pass

            for num in keys:
              
                skip_obs = False

                if source in multi_file:
                    obs, meta = { "file" : ID, "priority" : priority }, {}
                    #TODO only update station info in dev mode, not operational!
                    for si in station_info:
                        try:                   meta[si] = get_bufr( bufr, num, si )
                        except Exception as e: meta[si] = None

                if meta["latitude"] in null_vals or meta["longitude"] in null_vals:
                    continue
                if meta["shortStationName"] not in null_vals and len(meta["shortStationName"]) == 4:
                    meta["stID"] = meta["shortStationName"]
                elif meta["stationNumber"] not in null_vals and meta["blockNumber"] not in null_vals:
                    meta["stID"] = str(meta["stationNumber"] + meta["blockNumber"]*1000).rjust(5, "0")
                else: continue

                if meta["stID"] in null_vals or meta["stationOrSiteName"] in null_vals: continue

                if (meta["stID"] not in known_stations()) and (len(meta["stationOrSiteName"]) > 1):
                    meta["updated"] = dt.utcnow()
                    if verbose: print("Adding", meta["stationOrSiteName"], "to database...")
                    try: cur.execute( sql_insert( "station", meta ) )
                    except Exception as e:
                        if verbose: print(e)

                for key in keys[num]:
                    #if skip_obs == True: break
                    try:    cur.execute(f'ALTER TABLE obs ADD COLUMN "{key[:64]}"')
                    except: pass
                    #max length of mysql identifier is 64!
                    #TODO: write param names and unit conversion dictionary
                    try: value = get_bufr( bufr, num, key )
                    except Exception as e:
                        if verbose: print(f"{e}: {key}")
                    if value in null_vals: value = None
                    obs[key[:64]] = value

                obs["stID"]    = meta["stID"]
                obs["updated"] = dt.utcnow()

                #we need correct date/time information, otherwise skip this obs!
                if source in multi_file:
                    for tk in time_keys[:4]:
                        if tk not in obs or obs[tk] in null_vals:
                            skip_obs = True; break
                    if skip_obs: continue
                
                    #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
                    conflict = "stID, " + ", ".join(time_keys) 
                    sql = sql_insert( "obs", obs, conflict=conflict, skip_update=list(time_keys)+["stID"] )
                    try:
                        cur.execute( sql )
                        set_file_status( FILE, "parsed" )
                        parsed_counter += 1
                    except Exception as e:
                        if verbose: print(e)
                        set_file_status( FILE, "error" )

            if source in multi_file:
                if parsed_counter == 0:
                    set_file_status( FILE, "empty" )
            else:
                if source == "RMI":     obs["year"] = FILE[11:15]
                elif source == "COD":   obs["year"] = FILE[0:2]
                else:                   obs["year"] = dt.utcnow().year
                #insert obsdata to db; on duplicate key update only obs values; no stID or time_keys
                conflict = "stID, " + ", ".join(time_keys)
                sql = sql_insert( "obs", obs, conflict=conflict, skip_update=list(time_keys)+["stID"] )
                try:
                    cur.execute( sql )
                    set_file_status( FILE, "parsed" )
                except Exception as e:
                    if verbose: print(e)
                    set_file_status( FILE, "error" )

        db.commit()
        ec.codes_release(bufr) #release file to free memory
        process     = psutil.Process(os.getpid())
        memory_used = process.memory_info().rss  // 1024**2
        memory_free = psutil.virtual_memory()[1] // 1024**2

        #TODO: remove this nasty workaround after memory leak is fixed!
        #if less than x MB free memory: commit, close db connection and restart program
        if memory_free <= config_script["min_ram"]:
            print("Too much RAM used, RESTARTING...")
            close_db(db, cur)
            exe = sys.executable #restart program
            os.execl(exe, exe, * sys.argv); sys.exit()

for SOURCE in config_sources:
    if verbose: print(f"Parsing source {SOURCE}...")
    parse_all_bufrs( SOURCE )

#commit to db and close all connections
close_db(db, cur)
