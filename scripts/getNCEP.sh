#!/bin/bash

# Script to download NCAR data files from the NCEP NOMADS server

mkdir -p NCEP

wget -nc -nd -np -r -P NCEP -A ".bufr_d" https://nomads.ncep.noaa.gov/pub/data/nccf/com/obsproc/prod/
#wget -nc -nd -np -r -P NCEP -A ".nr" https://nomads.ncep.noaa.gov/pub/data/nccf/com/obsproc/prod/
