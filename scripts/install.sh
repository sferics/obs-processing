#!/bin/bash

# installing eccodes from apt sources is not necessary when using conda
#sudo apt install libeccodes*
sudo apt install python python-dev wget

# clone plbufr repository (fork of pdbufr using polars instead of pandas)
git clone https://github.com/sferics/plbufr/

# create new conda environment, using package list
conda env create -f config/obs_env.yml

# install plbufr package (legacy install method!)
cd plbufr && python setup.py install

# install packaged files for easier imports
#python -m pip install --editable package

# change path of git hooks in local git config to .githooks
git config --local core.hooksPath .githooks/

# compile all .py files to speed-up first run of any script
python -m compileall

# execute sql scripts creating file_table in main database and altering station table (baro_height)
sqlite3 main.db < sql/file_table.sql
sqlite3 main.db < sql/station_table.sql
