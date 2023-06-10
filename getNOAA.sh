#!/bin/bash

#https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/

mkdir -p NOAA

wget -nc -nd -np -r -l1 -P NOAA -A ".bin" https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/
