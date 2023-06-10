#!/bin/bash

#https://opendata.meteo.be/ftp/observations/synop/

mkdir -p RMI

wget -nc -nd -r --no-parent -P RMI -A ".bufr" https://opendata.meteo.be/ftp/observations/synop/
