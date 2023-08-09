#!/bin/bash

dir="/home/juri/data/live/dwd/bufr/germany"
mkdir -p $dir

wget -nc -nd -np -e "robots=off" -r -l 1 -P $dir -A ".bin" https://opendata.dwd.de/weather/weather_reports/synoptic/germany/
