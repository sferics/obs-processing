**How to install OBS processing**
run install.sh
$ ./install.sh
or if the permissions are not set
$ bash install.sh

The install.sh script will install conda if not present, create and environment with all necessary packages, install the plbufr package from github as well as the local directory "package" using "python setup.py install".
It then defines ".githook" as the directory for git hooks. There is currently only one git hook which automatically installs the directory "package" as a package after each commit, so syntax errors can be easily avoided.
Afterwards, it will compile all .py files in the directory in order to speed-up the first run of each script.
Lastly, it executes 2 .sql files which add some essential tables and columns to the main database. These changes should be implemented in amalthea/main for a better integration.

**How to use OBS processing**

**decode_bufr.py**

This script decodes one or several BUFR files and inserts all relevant observations into the raw databases.
It can also process intire source/dataset directories which can be provided by the source name as arguments or via the configuration file's "source:" section.

It may use 5 different approaches ("-a", "--approach") to decode the files:
- pd: Using pdbufr package officially provided by ECMWF (very slow because it uses pandas)
- pl: Using plbufr package forked from pdbufr by sferics (faster because it uses polars instead)
- gt: Also using plbufr bufr but instead of creating a dataframe it uses a generator (equally fast)
- us: Fastest decoding method using bufr keys from ECCODES but lacking some observations like soil temperatures
- ex: Slower than "us" method but significantly faster than pdbufr/plbufr methods. Not guaranteed to work with all files, still lacking some information from DWD Open Data BUFR files

Example usages:

single file, redo even if already processed:
decode\_bufr.py -a pl -f example\_file.bufr -r

multiple files, use "," as divider character, show verbose output:
decode\_bufr.py -a ex -F example\_file1.bin,example\_file2.bin,example\_file3.bin -D "," -v

single source, consider only specific stations:
decode\_bufr.py DWD -a gt -k 10381,10382,10384,10385

multiple sources, process a maximum of 100 files per source:
decode\_bufr.py DWD KNMI RMI -a gt -n 100

custom config file, process all sources which are defined there and use custom output directory:
decode\_bufr.py -C obs\_custom.yml -O /custom/output/directory

**forge_obs.py**
This is a chain script

**reduce_obs.py**

**derive_obs.py**

**aggregate_obs.py**

**audit_obs.py**

**empty_obs.py**

**export_obs.py**


**Description of the configuration YAML files/structure in config/ directory**

codes/
        bufr/
                flags_{approach}.yml
                sequences.yml
synop.yml
metar.yml

element\_aggregation.yml

element\_info.yml

environment.yml
- conda environment information (packages to install)

obs\_template.yml
- main configuration file template with the following sections

	general:
	  - 
	database:
	  - 
	bufr:
	  - 
	obs:
	  - 
	scripts:
	  - 
	clusters:
	  - 
	sources:
	  - 

translations/
	bufr/
		{approach}.yml
	metwatch.yml
	imgw.yml
	{other_source}.yml

parser\_args.yml

station\_tables/
	{mode}_{stage}.yml
