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
The settings defined by command line arguments always overwrite settings defined in the script's configuration.

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
##### -k/--known\_stations

##### -c/--clusters

##### -m/--max\_retries

##### -n/--max\_files

##### -m/--mode

##### -s/--stage

##### -o/--timeout

##### -O/--output

##### -P/--processes

##### -T/--translation $TRANSLATION\_FILE
- define name of custom (BUFR) translation file


### decode\_bufr.py
This script decodes one or several BUFR files and inserts all relevant observations into the raw databases.\
It can also process intire source/dataset directories which can be provided by the source name as arguments or via the configuration file's "source:" section.\
By default, the configuration file's name is defined as "obs.yml". So before the first usage, you need to make sure to create it by copying the "obs\_template.yml" in "config/" and adding your desired configurations/sources.


#### Unique command line arguments explained in detail

##### source

##### -a/--approach
You may use 5 different approaches to decode the files:
- pd: Using pdbufr package officially provided by ECMWF (very slow because it uses pandas)
- pl: Using plbufr package forked from pdbufr by sferics (faster because it uses polars instead)
- gt: Also using plbufr bufr but instead of creating a dataframe it uses a generator (equally fast)
- us: Fastest decoding method using bufr keys from ECCODES but lacking some observations like soil temperatures
- ex: Slower than "us" method but significantly faster than pdbufr/plbufr methods. Not guaranteed to work with all files, still lacking some information from DWD Open Data BUFR files
#### 

#### -D/--divider

#### -r/--redo

#### -R/--restart

#### -s/--sort\_files

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
#### Unique command line arguments explained in detail

### reduce\_obs.py
TODO
#### Unique command line arguments explained in detail

### derive\_obs.py
TODO
#### Unique command line arguments explained in detail

### aggregate\_obs.py
TODO
#### Unique command line arguments explained in detail

### derive\_obs.py -A
TODO
#### Unique command line arguments explained in detail

### audit\_obs.py
TODO
#### Unique command line arguments explained in detail

### empty\_obs.py
TODO
#### Unique command line arguments explained in detail

### export\_obs.py
TODO
#### Unique command line arguments explained in detail

1 reduce\_obs.py (only 1 row with max(file) per dataset [UNIQUE datetime,duration,element])
  copy all remaining elements from raw to forge databases [datetime,duration,element,value]
-in forge databases do:
2 audit\_obs.py      ->  check each obs, delete bad data like NaN, unknown value or out-of-range
2 derive\_obs.py     ->  compute derived elements like RH+TMP=DPT; cloud levels; reduced pressure...
3 aggregate\_obs.py  ->  aggregate over time periods (1,3,6,12,24h) and create new elements with "\_DUR" suffix
4 derive\_obs.py -A  ->  compute derived elements again, but only 30min values (--aggregated)
5 audit\_obs.py      ->  check each obs, delete bad data like NaN, unknown value or out-of-range
                        move good data in file databases e.g. "/oper/final" (oper mode)
                        move bad data to seperate databases, e.g. "/dev/bad" directory (dev mode)
6 empty\_obs.py      ->  clear forge databases (they are temporary and get rebuilt every chain cycle)

## Configuration YAML files/structure in "config/" directory

### codes/
        bufr/\
                flags_{approach}.yml\
                sequences.yml\
### synop.yml
### metar.yml
### element\_aggregation.yml
### element\_info.yml
### environment.yml
- conda environment information (environment name, packages to install, conda settings)
- does not contain prefix and variables because they are system-dependent
### obs\_template.yml
main configuration file template with the following sections:
> general:
>> - TODO
> database:
>> - TODO
> bufr:
>> - TODO
> obs:
>> - TODO
> scripts:
>> - TODO
> clusters:
>> - TODO
> sources:
>> - TODO

### translations/
	bufr/\
		{approach}.yml\
		- BUFR key translations for the different approaches
	metwatch.yml\
	- translation for the legacy metwatch element names
	imgw.yml\
	- translation for element names of Polish weather service Open Data
	{other_source}.yml\
	- use this naming scheme if you want to add your own custom source translation files

### parser\_args.yml
### station\_tables/
	{mode}_{stage}.yml

## Bash scripts in "scripts/" directory

### export\_bufr\_tables.sh
- TODO
### export\_conda\_env.sh
- TODO
### install.sh
- TODO
### multi\_decode\_bufr.sh
- TODO
