#!/bin/bash

for SRC in DWD RMI NOAA NCAR COD OGIMET; do
	bash get${SRC}.sh
done

for src in knmi swiss smhi imgw; do
	python get_${src}.py
done

