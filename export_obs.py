#!/usr/bin/env python
#exporter for MSwr old MOS system
from global_variables import null_vals
from database import DatabaseClass
from config import ConfigClass

#TODO write converter from database to MSwr/metwatch format

def read_obs( path ):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    # read obs from csv files and store into dictionary
    pass


def export_obs( data, output, fmt="csv" ):
    """
    Parameter:
    ----------

    Notes:
    ------

    Return:
    -------

    """
    import numpy as np
    from datetime import datetime as dt, timedelta as td
    import psutil, os, sys, pathlib, gzip, subprocess
    from shutil import move, copyfileobj, copyfile
    
    to_C = lambda kelvin : str(int(( float(kelvin) - 273.15)*10))

    to_kt = lambda ms : str(int( (900/463) * float(str(ms)) )) if ms not in null_vals else None

    to_hPa = lambda Pa : str(np.round( Pa/100, 1 ))

    n = lambda x : x

    def to_type(station):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        d = {0: "SYNO"}
        return d[station]


    def to_lat(lat):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        try: lat = float(lat) 
        except: return "/"
        celestial = "N" if lat > 0 else "S"
        deg = str(abs(int(lat)))
        if len(deg) < 2:
            deg = "0" + deg
        minutes = str( abs(int( np.round((lat-int(lat))*100, 2)) ) )
        if len(minutes) <2:
            minutes += "0"
        
        return celestial + deg + " " + minutes


    def to_lon(lon):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        try: lon = float(lon)
        except: return "/"
        celestial = "E" if lon > 0 else "W"
        deg = str(abs(int(lon)))
        while len(deg) < 3:
            deg = "0" + deg
        minutes = str( abs(int( np.round((lon-int(lon))*100, 2)) ) )
        if len(minutes) <2:
            minutes += "0"

        return celestial + deg + " " + minutes


    def get_first( key ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        h1 = list(data[DATE][station].keys())[0]
        m1 = list(data[DATE][station][h1].keys())[0]
        t1 = list(data[DATE][station][h1][m1].keys())[0]
        return data[DATE][station][h1][m1][t1][key]


    def get_any( key ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        for mins in data[DATE][station][hour]:
            for time in data[DATE][station][hour][mins]:
                try:
                    val = data[DATE][station][hour][mins][time][key]
                    return val if type(val) == str else ""
                except: continue
        return ""


    def get_timeperiod( key, time ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        #at 00, specific timeperiod
        try: return str(data[DATE][station][hour]["00"][time][key])
        except: return ""


    def get_mins( key, mins ):
        """
        Parameter:
        ----------

        Notes:
        ------

        Return:
        -------

        """
        #any timeperiod
        try:
            for time in data[DATE][station][hour][mins]:
                try: return str(data[DATE][station][hour][mins][time][key])
                except: continue
            return ""
        except:
            return ""


    times = []

    #if fmt == "csv":
    #elif fmt == "rpl":
    #else: print( "Unknown format!" ); return

    ext = ".csv.gz"

    for DATE in data:

        #create folder for DATE if not exists
        outpath = output + "bufr" + DATE
        pathlib.Path(outpath).mkdir(parents=True, exist_ok=True)

        #copy INDEX and UNITS file to path
        for i in ("INDEX","UNITS"):
            copyfile("bufr" + i + ext, outpath + "/" + "bufr" + i + ext)

        #write data for each station and date in folders
        for station in data[DATE]:

            filename = output + "bufr" + str(DATE) + "/bufr" + station + ext

            #remove file if exists
            #TODO: just add new data to file and don't write it from scratch
            if os.path.exists(filename):
                os.remove(filename)

            time_start = dt.now()

            with gzip.open( filename, "wt", encoding="utf8" ) as o:
               
                #GMA station output files have 10min frequency
                if get_first( "type" ) == "SYNA":
                    for hour in sorted(data[DATE][station].keys()):
                        for mins in sorted(data[DATE][station][hour].keys()):
                            print(hour, mins)
                    continue

                #else: hourly
                for hour in sorted(data[DATE][station].keys()):
                    timecode = DATE + hour + "00"

                    line = []
                    #INDEX (station number)
                    line.append( station )
                    #LOCATOR
                    line.append( "" )
                    #TYPE
                    line.append( get_any( "type" )  )
                    #C (CCX correction)
                    line.append( "" )
                    #NAME (station name)
                    line.append( get_any( "stationOrSiteName" ) )

                    #LAT, LON, ELEV
                    for i in ("latitude", "longitude", "elevation"):
                        val = get_any( i )
                        if i == "latitude":
                            val = to_lat( val )
                        elif i == "longitude":
                            val = to_lon( val )
                        line.append( val )

                    #YYYYMMDDhhmm (timecode)
                    line.append( timecode )

                    #DIR (wind direction 1h)
                    line.append( get_timeperiod( "windDirection", "1h" ) )
                    #??? maximumWindGustDirection

                    #K (???)
                    line.append("")

                    #FF (wind speed 10 min average)
                    key = "maximumWindSpeed10MinuteMeanWind"
                    try: line.append( to_kt( get_any( key ) ) )
                    except: line.append("")

                    #FG (wind speed at reference period)
                    line.append( to_kt( get_any( "windSpeed" )) )

                    #Gh1 (gusts 1h)
                    line.append("")

                    #G10 (gusts 10 mins
                    key = "maximumWindSpeed10MinuteMeanWind"
                    try: line.append( to_kt( get_timeperiod( key, "1h") ) )
                    except: line.append("")

                    #FX (gusts, max 10 mins mean during reference period)
                    line.append("")

                    #FX1 (gusts, max 10 mins mean during last hour)
                    line.append("")

                    #FFM (mean wind during last hour)
                    line.append("")

                    #TL, TD, T5, TX, TN
                    for i in ("airTemperature","dewpointTemperature","soilTemperature", 
                            "maximumTemperatureAtHeightAndOverPeriodSpecified",
                            "minimumTemperatureAtHeightAndOverPeriodSpecified"):
                        try: line.append( to_C( get_timeperiod( i, "1h" ) ) )
                        except: line.append("")

                    #TX6, TN6, TX1, TN1
                    for tp in ("6h", "6h"):
                        for key in ("maximumTemperatureAtHeightAndOverPeriodSpecified",
                                "minimumTemperatureAtHeightAndOverPeriodSpecified"):
                            try: line.append( to_C( get_timeperiod( key, tp ) ) )
                            except: line.append("")

                    #TN5 (24h minimum of 5cm temperature)
                    line.append("")

                    #12 hour?
                    key = "groundMinimumTemperaturePast12Hours"
                    try: line.append( to_C(get_timeperiod( key, "12h" ) ) )
                    except: line.append("")
                    
                    #TW (water temperature)
                    line.append("")

                    #Tsfc (surface temperature)
                    line.append("")

                    #T-05 -> T-50 (surface temperature -XXcm)
                    for cm in ("05", "10", "20", "50"):
                        line.append( "" )

                    #T-1m (surface temperature 1m)
                    line.append("")

                    #T-2m (surface temperature 2m)
                    line.append("")

                    #QFE
                    key = "nonCoordinatePressure"
                    #or "pressure"???
                    try: line.append( to_hPa( get_timeperiod( key, "1h" ) ) )
                    except: line.append("")

                    #QFF
                    key = "pressureReducedToMeanSeaLevel"
                    try: line.append( to_hPa( get_timeperiod( key, "1h" ) ) )
                    except: line.append("")

                    #QNH
                    key = "nonCoordinateGeopotentialHeight"
                    try: line.append( to_hPa( get_timeperiod( key, "1h" ) ) )
                    except: line.append("")

                    #AP (pressure change)
                    line.append("")

                    #A (type of pressure change)
                    line.append("")

                    #www
                    key = "presentWeather"
                    line.append( get_timeperiod( key, "1h" ) )

                    #960 (additional weather)
                    line.append("")

                    #962 (more additional weather)
                    line.append("")

                    #963 (even more additional weather)
                    line.append("")

                    #MetarWX (significant weather and cloud cover)
                    line.append("")

                    #W1
                    key = "pastWeather1"
                    line.append( get_timeperiod( key, "1h" ) )

                    #W2
                    key = "pastWeather2"
                    line.append( get_timeperiod( key, "1h" ) )

                    #WWZ (additional weather / national group)
                    key = "otherWeatherPhenomena"
                    line.append( get_timeperiod( key, "1h" ) )

                    #LTNG (lightning count)
                    line.append( "" )

                    #VIS (horizontal visibility)
                    key = "horizontalVisibility"
                    try:
                        line.append( int( float( get_timeperiod( key, "1h" ) ) ) // 1000 )
                    except: line.append("")
                    
                    #COV (qualitative cloud cover)
                    key = "cloudCoverTotal"
                    line.append( get_timeperiod( key, "1h" ) )

                    #N (cloud amount in octas)
                    key = "cloudAmount"
                    line.append( get_timeperiod( key, "1h" ) )

                    #L (cloud amount of low clouds)
                    line.append("")

                    #HLC (height of lowest cloud)
                    line.append("")
                    
                    #ClCmCh
                    line.append("")
                    
                    #LAYER1-4
                    for i in range(4):
                        line.append("")

                    #CLG (ceiling)
                    line.append("")

                    #PIC (cover of mountains)
                    line.append("")

                    #VCLOUD1-4
                    for i in range(4):
                        line.append("")

                    #VFOG (valley fog and mist)
                    line.append("")

                    #RR (precipitation in reference period)
                    line.append("")

                    #TR (reference period in hours)
                    line.append("")

                    #RR10m
                    line.append("")

                    #RR1h
                    line.append("")

                    #hh
                    line.append("")

                    #RR24h
                    line.append("")

                    #WRTR
                    line.append("")

                    #SNO
                    line.append("")

                    #NEW
                    line.append("")

                    #SR
                    line.append("")

                    #GS
                    line.append("")

                    #SS24
                    line.append("")

                    #Sh
                    line.append("")

                    #S10
                    line.append("")

                    #GL24
                    line.append("")

                    #DF24
                    line.append("")

                    #LW24
                    line.append("")

                    #GL1h
                    line.append("")

                    #DF1h
                    line.append("")

                    #LW1h
                    line.append("")

                    #GL10
                    line.append("")

                    #DF10
                    line.append("")

                    #LW10
                    line.append("")

                    #HW
                    line.append("")

                    #PW
                    line.append("")

                    #DW1
                    line.append("")

                    #HW1
                    line.append("")

                    #P1
                    line.append("")

                    #DW2
                    line.append("")

                    #HW2
                    line.append("")

                    #P2
                    line.append("")

                    #HS
                    line.append("")

                    #PS
                    line.append("")

                    #I
                    line.append("")
                    
                    #B
                    line.append("")

                    #INDEX;LOCATOR;TYPE;C;NAME                          ;LAT   ;LON    ;ELEV;YYYYMMDDhhmm;DIR;K; FF; FG;G1h;G10; FX;FX1;FFM;  TL;  TD;  T5;  TX;  TN; TX6; TN6; TX1; TN1; TN5;  TW;Tsfc;T-05;T-10;T-20;T-50;T-1m;T-2m;   QFE;   QFF; QNH;  AP;A;www;960;961;962;963;           MetarWX;W1;W2;     WWZ; LTNG; VIS;COV;N;L;HLC;   ClCmCh;LAYER1;LAYER2;LAYER3;LAYER4;CLG;     PIC;VCLOUD1;VCLOUD2;VCLOUD3;VCLOUD4;  VFOG;   RR;TR;RR10m; RR1h;hh;RR24h;WRTR;SNO;NEW;SR;GS;SS24;Sh;S10;GL24;DF24;LW24;GL1h;DF1h;LW1h;GL10;DF10;LW10;  HW;PW;DW1; HW1;P1;DW2; HW2;P2;  HS;PS;I;B;

                    #103810;      /;SYNO;A;Berlin/Dahlem                 ;N52 27;E013 18;  51;202104291800;280;/; 04; 45;  /;  /;  /;  /;  /; 108;  63;   /; 170;  81;   /;   /;   /;   /;  50;   /;   /;   /;   /;   /;   /;   /;   /; 993.5;1003.1;   /;  17;2;  3;  /;  /;  /;  /;                 /; 9; 8;       /;    /;  45;FEW;3;1; 49;Cu2Ac7Ci1;1CU049;3AC100;     /;     /;FEW;       /;      /;      /;      /;      /;     /;    7;12;    /;    /; /;    /;   /;  0;  /; /; 1;   /;42;  /;   /;   /;   /;   /;   /;   /;   /;   /;   /;   /; /;  /;   /; /;  /;   /; /;   /; /;/;/;

                    formats = [":>6",":>7",":>4",":>1",":<30",":>6",":>6",":>4",":>12",":>3",":>1"]+7*[":>3"]+\
                            18*[":>4"] + 2*[":>5"] + 2*[":>4"] + [":>1"] + 5*[":>3"] + [":>18"] + 2*[":>2"] + \
                            [":>8", ":>5", ":>4", ":>3"] + 2*[":>1"] + [":>3", ":>9"] + 4*[":>6"] + \
                            [":>3", ":>8"] + 4*[":>7"] + [":>6", ":>5", ":>2"] + 2*[":>5"] + [":>2"] + \
                            [":>5", ":>4"] + 2*[":>3"] + 2*[":>2"] + [":>4", ":>2", ":>3"] + \
                            10*[":>4"] + [":>2", ":>3", ":>4", ":>2", ":>3", ":>4", ":>2", ":>4", ":>2"] + 2*[":>1"]

                    for i in range(len(line)):
                        line[i] = ("{"+formats[i]+"}").format( line[i] if line[i] not in null_vals else "/" )

                    line.append("")

                    print(";".join(line) + "\n")

                    o.export_obs( ";".join(line) + "\n" )

                o.close()

            time_end  = dt.now()
            time_diff = str(time_end - time_start )[-9:]
            times.append( float(time_diff) )
            """
            os.system("clear")
            if len(times) > 0:
                print("WRITING...")
                print("max:    %1.3f s" % np.round( np.max(times), 3) )
                print("min:    %1.3f s" % np.round( np.min(times), 3) )
                print("mean:   %1.3f s" % np.round( np.mean(times), 3) )
                print("median: %1.3f s" % np.round( np.median(times), 3) )
            """


if __name__ == "__main__":
    
    # define program info message (--help, -h)
    info        = "Export observations to legacy output format (metwatch csv)"
    script_name = gf.get_script_name(__file__)
    flags       = ("l","v","C","m","M","o","O","L","e","d","t","P")
    cf          = ConfigClass(script_name, pos=["source"], flags=flags, info=info, verbose=True)
    log_level   = cf.script["log_level"]
    log         = gf.get_logger(script_name, log_level=log_level)
    start_time  = dt.utcnow()
    started_str = f"STARTED {script_name} @ {start_time}"

    log.info(started_str)

    # define some shorthands from script config
    verbose         = cf.script["verbose"]
    debug           = cf.script["debug"]
    traceback       = cf.script["traceback"]
    timeout         = cf.script["timeout"]
    max_retries     = cf.script["max_retries"]
    mode            = cf.script["mode"]
    output          = cf.script["output"] + "/" + mode
    legacy_output   = cf.script["legacy_output"]
    clusters        = cf.script["clusters"]
    stations        = cf.script["stations"]
    processes       = cf.script["processes"]

    obs             = ObsClass( cf, source, stage="forge" )
    db              = DatabaseClass( config=cf.database, ro=1 )
    stations        = db.get_stations( clusters )
    db.close(commit=False)

    with open("output.json") as f:
        INPUT = json.load( f )
        export_obs( INPUT, "output/" )
