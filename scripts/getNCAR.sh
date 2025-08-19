#!/bin/bash

# Script to download NCAR data files from the NCEP NOMADS server

mkdir -p NCAR

# Download BUFR data files for current date and time

#TODO test this ChatGPT proposal
#URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/ncar/prod/"
#URL="$(curl -s $URL | grep -oP 'href="\K[^"]*' | grep 'ncar\.t[0-9]{2}z\.bufr\.tm[0-9]{2}\.pgrb2\.f[0-9]{3}\.nc' | head -n 1)"

URL="https://data.rda.ucar.edu/ds461.0/bufr/{YYYY}/gdas.adpsfc.t{ZZ}z.{YYYY}{MM}{DD}.bufr"

# Replace placeholders with actual date values
# Note: The date command outputs in UTC, adjust if necessary for your timezone
# YYYY, MM, DD, and ZZ are placeholders for year, month, day, and hour
# Assuming the hour is in UTC and formatted as two digits (00-23)

# get yesterday's date in UTC
YESTERDAY=$(date -u -d "yesterday" +"%Y-%m-%d")
# Extract year, month, and day from yesterday's date
YYYY=$(date -u -d "$YESTERDAY" +"%Y")
MM=$(date -u -d "$YESTERDAY" +"%m")
DD=$(date -u -d "$YESTERDAY" +"%d")

# Make a loop and try to download the 6 hourly files for the previous day
for i in {0..23..6}; do
    # Calculate the hour in UTC, ensuring it is two digits
    HH=$(printf "%02d" $((10#$i % 24)))
    URL="https://data.rda.ucar.edu/ds461.0/bufr/${YYYY}/gdas.adpsfc.t${HH}z.${YYYY}${MM}${DD}.bufr"
    echo "Downloading: $URL"
    wget -q -P NCAR "$URL"
done
