# How to install OBS processing
- run install.sh like this:\
`chmod +x install.sh && ./install.sh`
- or if the permissions cannot be set/changed:\
`bash install.sh`
- The install.sh script will install miniconda if not present, create an environment with all necessary packages and install the plbufr package from github as well as the local directory "package" using "python setup.py install".
- It then defines ".githook/" as the directory for git hooks. There is currently only one git hook which automatically installs the directory "package" as a package after each commit, so syntax errors can be easily avoided and exports the conda environment information to "config/environment.yml".
- Afterwards, it will compile all .py files in the directory in order to speed-up the first run of each script.
- Lastly, it executes 2 .sql files which add some essential tables and columns to the main database. These changes should be implemented in amalthea/main for a better integration.

# How to use OBS processing

## Python scripts
All python scripts offer a -h/--help option which shows their command line arguments with a brief explanation. However, in order to understand them better, you should read the following in-depth information carefully.

### Command line arguments

All command line arguments are defined in "config/parser\_args" and they are the same across all scripts. The only difference lies in their availability. For more details, read the section about the YAML configuration files.\
Please note: Settings defined by command line arguments always overwrite settings defined in the script's configuration.

#### Common command line arguments

##### -h/--help
- show help message (defined in last column of "config/parser\_args")
##### -v/--verbose
- print (more) verbose output
##### -d/--debug
- run in debug mode with additional debug prints and stop points (using pdb module)
##### -t/--traceback
- use traceback module to print error messages that occur on module level
##### -w/--no\_warnings
- supress all warnings
##### -i/--pid\_file
- use a PID file to determine whether the script is already running and which processes number it has
##### -l/--log\_level $LOG\_LEVEL
- define logging level (choose one of the following: {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET} )
##### -C/--config\_file $FILE\_NAME
- define a custom config file (has to be within "config/" directory)
##### -k/--known\_stations $LIST\_OF\_STATIONS
- comma-seperated list of stations to consider
##### -c/--clusters $LIST\_OF\_CLUSTERS
- comma-seperated list of clusters to consider
##### -m/--max\_retries $RETRIES
- maximal number of retries when writing to station databases
##### -n/--max\_files $NUMBER\_OF\_FILES
- maximal number of files to process (usually per source)
##### -m/--mode $MODE
- operation mode (can be "dev" or "oper")
##### -s/--stage $STAGE
- stage of forging (can be "raw","forge","bad" or "final")
##### -o/--timeout $TIMEOUT
- timeout in seconds when trying to write to station databases
##### -O/--output $OUTPUT\_PATH
- define custom output path
##### -P/--processes $NUMBER\_OF\_PROCESSES
- use multiprocessing if -P > 1; defines number of processes to use
##### -T/--translation $TRANSLATION\_FILE
- define name of custom (BUFR) translation file


### decode\_bufr.py
This script decodes one or several BUFR files and inserts all relevant observations into the raw databases.\
It can also process intire source/dataset directories which can be provided by the source name as arguments or via the configuration file's "source:" section.\
By default, the configuration file's name is defined as "obs.yml". So before the first usage, you need to make sure to create it by copying the "obs\_template.yml" in "config/" and adding your desired configurations/sources.


#### Unique command line arguments

##### source (first and only positional argument, can take several)

##### -a/--approach
You may use 5 different approaches to decode the files:
- pd: Using pdbufr package officially provided by ECMWF (very slow because it uses pandas)
- pl: Using plbufr package forked from pdbufr by sferics (faster because it uses polars instead)
- gt: Also using plbufr bufr but instead of creating a dataframe it uses a generator (equally fast)
- us: Fastest decoding method using bufr keys from ECCODES but lacking some observations like soil temperatures
- ex: Slower than "us" method but significantly faster than pdbufr/plbufr methods. Not guaranteed to work with all files, still lacking some information from DWD Open Data BUFR files
##### -f/--file $FILE\_PATH
- process single file by file path
##### -F/--FILES $LIST\_OF\_FILES
- process several files, given by file paths, seperated by divider character (default: ";")
##### -D/--divider $DIVIDER
- define a custom divider/seperator character for -F
##### -r/--redo
- process file(s) again even if they have been processed already
##### -R/--restart
- usually only used automatically by the script if the RAM is full, so it knows which files are still left to process
##### -s/--sort\_files
- sort files by sorting algorithm (sorted() by default)

#### Example usages

##### single file, redo even if already processed:
`decode_bufr.py -a pl -f example_file.bufr -r`

##### multiple files, use "," as divider character, show verbose output:
`decode_bufr.py -a ex -F example_file1.bin,example_file2.bin,example_file3.bin -D "," -v`

##### single source, consider only specific stations:
`decode_bufr.py DWD -a gt -k 10381,10382,10384,10385`

##### multiple sources, process a maximum of 100 files per source:
`decode_bufr.py DWD KNMI RMI -a gt -n 100`

##### custom config file, process all sources which are defined there and use custom output directory:
`decode_bufr.py -C obs_custom.yml -O /custom/output/directory`

### forge\_obs.py
This is a chain script which runs the following scripts in the order of occurrence. Only in operational mode, derived\_obs.py runs again after aggregate\_obs.py and export\_obs.py will only be executed if -e/--export is set.
#### Unique command line arguments

### reduce\_obs.py
(only 1 row with max(file) per dataset [UNIQUE datetime,duration,element])
Copy all remaining elements from raw to forge databases [dataset,datetime,duration,element,value]

#### Unique command line arguments

### derive\_obs.py
Compute derived elements like relative humidity, cloud levels or reduced pressure.

#### Unique command line arguments

### aggregate\_obs.py
Aggregate over certain time periods (like 30min,1h,3h,6h,12,24h) and create new elements with "\_DUR" suffix.

#### Unique command line arguments

### derive\_obs.py -A
Compute derived elements again, but only considering 30min-values.

### audit\_obs.py
Check all obs in forge databases, delete bad data like NaN, unknown value or out-of-range
- move good data in file databases e.g. "/oper/final" (oper mode)
- move bad data to seperate databases, e.g. "/dev/bad" directory (dev mode)

#### Unique command line arguments

### empty\_obs.py
Clear forge databases (they are temporary and get rebuilt every chain cycle).

#### Unique command line arguments

### export\_obs.py
Export observations from final databases into the old/legacy metwatch csv format.

#### Unique command line arguments


## Configuration YAML files/structure in "config/" directory

### codes/
> #### bufr/
> > ##### flags_{approach}.yml
> > \- TODO
> > ##### sequences.yml
> > \- TODO
> ##### synop.yml
> \- TODO
> ##### metar.yml
> \- TODO

##### element\_aggregation.yml
\- TODO

##### element\_info.yml
\- TODO

##### environment.yml
\- conda environment information (environment name, packages to install, conda settings)\
\- does not contain prefix and variables because they are system-dependent

##### obs\_template.yml
\- main configuration file template with the following sections:

> **general:**\
> \- most general settings which will be overwritten by all following configs\
> \- order: general -> class -> script -> command line arguments\
> **database:**\
> \- TODO\
> **bufr:**\
> \- TODO\
> **obs:**\
> \- TODO\
> **scripts:**\
> \- TODO\
> **clusters:**\
> \- TODO\
> **sources:**\
> \- TODO

### translations/
> #### bufr/
> > ##### {approach}.yml
> > \- BUFR key translations for the different approaches\
> ##### metwatch.yml
> \- translation for the legacy metwatch element names\
> ##### imgw.yml
> \- translation for element names of Polish weather service Open Data\
> ##### {other\_source}.yml
> \- use this naming scheme if you want to add your own custom source translation files\

##### parser\_args.yml
\- TODO
##### station\_tables/
> ##### {mode}\_{stage}.yml
> \- TODO

## Bash scripts in "scripts/" directory

### export\_bufr\_tables.sh
- TODO
### export\_conda\_env.sh
- TODO
### install.sh
- TODO
### multi\_decode\_bufr.sh
- TODO
#### Command line arguments
##### $1
- approach
##### $2
- number of processes to use (start decode\_bufr.py N times)
