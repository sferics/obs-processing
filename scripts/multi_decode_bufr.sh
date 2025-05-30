#!/bin/bash
# start poor-man's multi processing for the decode_bufr script.py by just starting it N times (default is 15)
# second cli argument defines "approach" setting (-a) of decode_bufr.py script (default: gt)

if [ $# -ge 1 ]
then
    N=$1
    a=$2
    s=$3
else
    N=15
    a=pl
    s=60
fi

python -m compileall
source scripts/export_bufr_tables.sh

echo "Starting decode_bufr.py $N times..."

for (( c=1; c<=$N; c++ )); do
	nohup python decode_bufr.py test -a $a & sleep $s
done
