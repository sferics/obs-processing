#!/bin/bash

#https://rda.ucar.edu/datasets/ds461.0/
#https://data.rda.ucar.edu/ds461.0/tarfiles/YYYY/gdassfcobs.YYYYMMDD.tar.gz

mkdir -p NCAR

DATE=$(date -d yesterday +%Y%m%d)
YYYY=${DATE:0:4}

wget -nc -nd -P NCAR -qO- https://data.rda.ucar.edu/ds461.0/tarfiles/${YYYY}/gdassfcobs.${DATE}.tar.gz | tar -xz -C NCAR
