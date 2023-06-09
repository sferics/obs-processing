#!/bin/bash

mkdir -p bufr
for region in germany international; do
	wget -nc -nd -r -l1 --no-parent -P bufr -A ".bin" https://opendata.dwd.de/weather/weather_reports/synoptic/$region/
done
