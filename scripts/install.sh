#!/bin/bash

# installing eccodes from apt sources is not necessary when using conda
#sudo apt install libeccodes*
sudo apt install python python-dev wget

#TODO automize miniconda install, custom installation directory, use zsh instead of bash
# https://docs.anaconda.com/free/miniconda/
miniconda_dir="~/miniconda3"
if [ ! -d "$miniconda_dir" ]; then
	mkdir -p "$miniconda_dir"
	wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "${miniconda_dir}/miniconda.sh"
	bash "{$miniconda_dir}/miniconda.sh" -b -u -p "$miniconda_dir"
	rm -rf "${miniconda_dir}/miniconda.sh"
	${miniconda_dir}/bin/conda init bash
	#${miniconda_dir}/bin/conda init zsh
fi

#TODO custom environment name (default=obs)
# create new conda environment, using package list
conda env create -f environment.yml

# clone plbufr repository (fork of pdbufr using polars instead of pandas)
git clone https://github.com/sferics/plbufr/

# install plbufr package (legacy install method!)
#cd plbufr && python setup.py install && cd ..
# install plbufr package (new method using pip()
cd plbufr && python setup.py install && pip install plbufr && cd ..

# permanently add modules directory to PYTHONPATH
conda env config vars set PYTHONPATH="${PYTHONPATH}:modules"

#TODO the above works fine but maybe use `conda develop` instead? https://docs.conda.io/projects/conda-build/en/latest/resources/commands/conda-develop.html
#conda install conda-build && conda develop modules

#TODO install packaged files (located in modules) for easier imports and syntax checks via git hook
#python -m pip install --editable modules
#python -m pip install -e modules

# change path of git hooks in local git config to .githooks
git config --local core.hooksPath .githooks/

# compile all .py files in order to detect more syntax errors and speedup first execution of scripts
# uses recursion depth 1 as described here: https://stackoverflow.com/a/59610560/12935487
python -m compileall . -lq
python -m compileall modules -q

# execute sql scripts creating file_table in main database and altering station table (baro_height)
sqlite3 main.db < sql/file_table.sql
sqlite3 main.db < sql/station_table.sql
# also delete all entries with role='obs' from element_table and insert all needed obs elements
sqlite3 main.db < sql/element_table.sql
