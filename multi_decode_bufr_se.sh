#!/bin/bash
# start poor-man's multi processing for the decode_bufr script.py by just starting it N times (default is 15)

if [ $# -ge 1 ]
then
    N=$1
else
    N=15
fi

echo "Starting decode_bufr_se.py $N times..."

for (( c=1; c<=$N; c++ )); do
	python decode_bufr_se.py test & sleep 2
done
