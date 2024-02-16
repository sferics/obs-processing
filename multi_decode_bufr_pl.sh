#!/bin/bash
# start poor-man's multi processing for the decode_bufr script.py by just starting it N times (default is 15)

if [ $# -ge 1 ]
then
    N=$1
else
    N=15
fi

python -m compileall
source export_bufr_tables.sh

echo "Starting decode_bufr_pl.py $N times..."

for (( c=1; c<=$N; c++ )); do
	nohup python decode_bufr_pl.py test & sleep 3
done

#python aggregate_obs.py && echo "finished" > multi_decoder_pd.py.log
