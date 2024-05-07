#!/bin/bash

#https://opendata.meteo.be/ftp/observations/synop/

dir="/home/juri/data/live/rmi/bufr"
mkdir -p $dir

wget -nc -nd -r --no-parent -P $dir -A ".bufr" https://opendata.meteo.be/ftp/observations/synop/
