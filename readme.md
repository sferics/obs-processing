# How to install OBS processing
run install.sh like this:\
$ chmod +x install.sh && ./install.sh\
or if the permissions cannot be set / changed:\
$ bash install.sh\
\
The install.sh script will install miniconda if not present, create an environment with all necessary packages and install the plbufr package from github as well as the local directory "package" using "python setup.py install".\
It then defines ".githook" as the directory for git hooks. There is currently only one git hook which automatically installs the directory "package" as a package after each commit, so syntax errors can be easily avoided and exports the conda environment information to "config/environment.yml".\
Afterwards, it will compile all .py files in the directory in order to speed-up the first run of each script.\
Lastly, it executes 2 .sql files which add some essential tables and columns to the main database. These changes should be implemented in amalthea/main for a better integration.\

# How to use OBS processing

## Python scripts
All python scripts offer a -h/--help option which shows their command line arguments with a brief explanation. However, in order to understand them better, you should read the following in-depth information carefully.

### decode\_bufr.py
This script decodes one or several BUFR files and inserts all relevant observations into the raw databases.\
It can also process intire source/dataset directories which can be provided by the source name as arguments or via the configuration file's "source:" section.\
By default, the configuration file's name is defined as "obs.yml". So before the first usage, you need to make sure to create it by copying the "obs\_template.yml" in "config/" and adding your desired configurations / sources.

#### Important flags explained in detail

##### --approach
It may use 5 different approaches ("-a", "--approach") to decode the files:
- pd: Using pdbufr package officially provided by ECMWF (very slow because it uses pandas)
- pl: Using plbufr package forked from pdbufr by sferics (faster because it uses polars instead)
- gt: Also using plbufr bufr but instead of creating a dataframe it uses a generator (equally fast)
- us: Fastest decoding method using bufr keys from ECCODES but lacking some observations like soil temperatures
- ex: Slower than "us" method but significantly faster than pdbufr/plbufr methods. Not guaranteed to work with all files, still lacking some information from DWD Open Data BUFR files

#### Example usages

##### single file, redo even if already processed:
decode\_bufr.py -a pl -f example\_file.bufr -r

##### multiple files, use "," as divider character, show verbose output:
decode\_bufr.py -a ex -F example\_file1.bin,example\_file2.bin,example\_file3.bin -D "," -v

##### single source, consider only specific stations:
decode\_bufr.py DWD -a gt -k 10381,10382,10384,10385

##### multiple sources, process a maximum of 100 files per source:
decode\_bufr.py DWD KNMI RMI -a gt -n 100

##### custom config file, process all sources which are defined there and use custom output directory:
decode\_bufr.py -C obs\_custom.yml -O /custom/output/directory

### forge\_obs.py
This is a chain script which runs the following scripts in the order of occurrence. Only in operational mode, derived\_obs.py runs again after aggregate\_obs.py and export\_obs.py will only be executed when -e/--export is set.
### reduce\_obs.py
- TODO
### derive\_obs.py
- TODO
### aggregate\_obs.py
- TODO
### derive\_obs.py -A
- TODO
### audit\_obs.py
- TODO
### empty\_obs.py
- TODO
### export\_obs.py
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
- main configuration file template with the following sections\
	general:\
	  - TODO\
	database:\
	  - TODO\
	bufr:\
	  - TODO\
	obs:\
	  - TODO\
	scripts:\
	  - TODO\
	clusters:\
	  - TODO\
	sources:\
	  - TODO\

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
