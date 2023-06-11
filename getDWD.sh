#!/bin/bash

mkdir -p DWD

for region in germany international; do
	wget -nc -nd -np -r -l1 -P DWD -A ".bin" https://opendata.dwd.de/weather/weather_reports/synoptic/$region/
done

wget -nc -nd -np -r -l1 -P DWD -A ".txt" https://opendata.dwd.de/weather/weather_reports/synoptic/international/
