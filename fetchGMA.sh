#!/bin/bash

mkdir -p "GMA"
wget -nc -nd -r --no-parent -P GMA -A "---bin" https://opendata.dwd.de/weather/weather_reports/road_weather_stations/
