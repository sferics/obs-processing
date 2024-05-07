#!/bin/bash

#https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/
#https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.synop/

mkdir -p NOAA

wget -nc -nd -np -r -l1 -P NOAA -A ".bin" https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/
wget -nc -nd -np -r -l1 -P NOAA -A ".txt" https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.synop/
