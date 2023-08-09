#!/bin/bash

#https://weather.cod.edu/digatmos/syn/
#https://weather.cod.edu/digatmos/BUFR/SYNOP/EDZW/

dir="/home/juri/data/live/cod/bufr"
mkdir -p $dir

wget -e robots=off -nc -nd -np -r -P $dir -A ".bufr" https://weather.cod.edu/digatmos/BUFR/SYNOP/EDZW/
