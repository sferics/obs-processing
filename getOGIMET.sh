#!/bin/bash

#http://www.ogimet.com/getbufr_help.phtml.en

OUTDIR="OGIMET"
TMPDIR="${OUTDIR}/TMP"

mkdir -p $TMPDIR

TD=$(date +%Y%m%d%H%M)
#YD=$(date -d yesterday +%Y%m%d)
YD=$(date -d "14 days ago" +%Y%m%d)

LST="${OUTDIR}/listing.html"
URL="https://www.ogimet.com/getbufr.php"

# Grep listing from ogimet (listing with files)
printf "Downloading file listing from ogimet\n"
if [ -f ${LST} ] ; then rm ${LST} ; fi
wget --inet4-only "${URL}?res=list&beg=${YD}0000&end=${TD}" -O "$LST"

regex=">\S+*\.bufr<"
nfiles=`cat ${LST} | egrep -oE $regex | wc -l`
files=`cat ${LST} | egrep -oE $regex | sed -e "s/<//g" -e "s/>//g"`

printf "Number of files to be considered: %d\n" $nfiles

if [ $nfiles -eq 0 ] ; then
    printf "No files, stop ...\n\n"
    exit 0
fi

# If a CCX/COR/RRX exists we should remove all filess but the latest correction
# TODO

# Loop over files and download if not yet available. If
# we download a new file: move the file to the incoming
# folder with additionals. These files will be considered.
# by the bufr decoder.
printf "Start downloading new bufr files (if there are any)\n"
for file in ${files[@]} ; do
    local=`printf "%s/%s" ${OUTDIR} ${file}`
    if [ ! -f ${local} ] ; then
        success=0
        ii=0
        while [ $success -eq 0 ] && [ $ii -lt 3 ]; do
            printf " - Downloading %s\n" ${file}
            wget --inet4-only "${URL}?file=${file}" -O ${local} || rm -f ${local}
            #delete file if size == 0byte
            if [ ! -s ${local} ]; then
               rm -f ${local}
               success=0
               echo "0 BYTE FILE!"
            fi
            if [ -f ${local} ]; then
               succes=1
               echo "SUCCESS!"
            else
               echo "FILE NOT DOWNLOADED!"
            fi
            ii+=1
        done
    else
        printf " - Already processed: %s\n" ${file}
    fi
done
