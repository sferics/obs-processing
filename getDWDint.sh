#!/bin/bash

mkdir -p bufr
wget -nc -nd -r -l1 --no-parent -P bufr -A ".bin" https://opendata.dwd.de/weather/weather_reports/synoptic/international/
