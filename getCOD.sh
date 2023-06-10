#!/bin/bash

#https://weather.cod.edu/digatmos/syn/
#https://weather.cod.edu/digatmos/BUFR/SYNOP/EDZW/

mkdir -p COD

wget -e robots=off -nc -nd -np -r -P COD -A ".syn" https://weather.cod.edu/digatmos/syn/
wget -e robots=off -nc -nd -np -r -P COD -A ".bufr" https://weather.cod.edu/digatmos/BUFR/SYNOP/EDZW/
