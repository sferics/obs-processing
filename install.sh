#!/bin/bash

# create symlink for undecided naming reasons...
ln -s conclude_obs.py finalize_obs.py

# clone plbufr repository (fork of pdbufr using polars instead of pandas)
git clone https://github.com/sferics/plbufr/

# create new conda environment, using package list
conda env create -f config/obs_env.yml

# install plbufr package (legacy install method!)
cd plbufr && python setup.py install

# compile all .py files to speed-up first start
python -m compileall
