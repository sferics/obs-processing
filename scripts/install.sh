#!/bin/bash

#sudo apt install libeccodes*
sudo apt install python python-dev wget

# create symlink for undecided naming reasons...
ln -s conclude_obs.py finalize_obs.py

# clone plbufr repository (fork of pdbufr using polars instead of pandas)
git clone https://github.com/sferics/plbufr/

# create new conda environment, using package list
conda env create -f config/obs_env.yml

# install plbufr package (legacy install method!)
cd plbufr && python setup.py install

# compile all .py files to speed-up first run of any script
python -m compileall

# execute sql script which creates file_table in main database
sqlite3 main.db < sql/file_table.sql
