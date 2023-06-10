#!/bin/bash

for SRC in DWD KNMI RMI NOAA COD; do
	bash get${SRC}.sh
done
