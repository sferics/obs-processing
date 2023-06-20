#!/bin/bash

for SRC in DWD RMI NOAA NCAR COD OGIMET; do
	bash get${SRC}.sh
done

for SRC in KNMI SWISS; do
	python get${SRC}.py
done

