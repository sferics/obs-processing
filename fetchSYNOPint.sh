#!/bin/bash

BASE_URL="https://opendata.dwd.de/weather/"
WHAT_WE_WANT="weather_reports/synoptic/international/"
CONTENT_LOG="content.log"
CONTENT_LOG_BZ2="$CONTENT_LOG.bz2"
CONTENT_LOG_OLD="$CONTENT_LOG.old"
TMP_BASE="fetchSYNOPint"
TODO_LIST="$TMP_BASE.todo"
LOCK_FILE="$TMP_BASE.lock"
DONE_LIST="$TMP_BASE.done"
BUFR_DIR="bufr/"
LOCAL_DATA_DIR="$BUFR_DIR/tmp"
PROCESSED_DIR="$BUFR_DIR/processed"

WGET="/usr/bin/wget"
BUNZIP2="/bin/bzip2 -d"

echo "[STARTED] fetchSYNOPint"
date

if [ -f $LOCAL_DATA_DIR/$LOCK_FILE ]
then
   echo "Script already running!"
   exit 1
fi

# Handle signals
trap 'exithandler' 0 1 2 15
exithandler() {
   rm -f $LOCAL_DATA_DIR/$LOCK_FILE $LOCAL_DATA_DIR/$TODO_LIST $LOCAL_DATA_DIR/$CONTENT_LOG
}

echo "$$" > $LOCAL_DATA_DIR/$LOCK_FILE
$WGET -q -O - ${BASE_URL}${CONTENT_LOG_BZ2} | $BUNZIP2 | egrep -v $CONTENT_LOG_BZ2 | sort > $LOCAL_DATA_DIR/$CONTENT_LOG

if [ ! -f $LOCAL_DATA_DIR/$CONTENT_LOG ]
then
   echo "Download failed"
   exit 1
fi

if [ -f $LOCAL_DATA_DIR/$CONTENT_LOG_OLD ]
then
   diff $LOCAL_DATA_DIR/$CONTENT_LOG $LOCAL_DATA_DIR/$CONTENT_LOG_OLD | egrep "<" | sed s/\<\ .// | cut -f 1 -d "|" | egrep $WHAT_WE_WANT | sed "s,^\/,$BASE_URL," > $LOCAL_DATA_DIR/$TODO_LIST
else
   cat $LOCAL_DATA_DIR/$CONTENT_LOG | cut -f 1 -d "|" | egrep $WHAT_WE_WANT | sed "s,^.\/,$BASE_URL," > $LOCAL_DATA_DIR/$TODO_LIST
fi

ls bufr/processed/ | cat > $LOCAL_DATA_DIR/$DONE_LIST

diff $LOCAL_DATA_DIR/$TODO_LIST $LOCAL_DATA_DIR/$DONE_LIST > "test.txt"

echo "TODO: $LOCAL_DATA_DIR/$TODO_LIST"
if [ -s $LOCAL_DATA_DIR/$TODO_LIST ]
then
   $WGET -N -nd -q -np -i $LOCAL_DATA_DIR/$TODO_LIST -P $BUFR_DIR || echo "Download failed!" && exit 1
fi

mv $LOCAL_DATA_DIR/$TODO_LIST $LOCAL_DATA_DIR/$DONE_LIST

mv $LOCAL_DATA_DIR/$CONTENT_LOG $LOCAL_DATA_DIR/$CONTENT_LOG_OLD

echo "Download succesful!"

echo "[FINISHED] fetchSYNOPint"
date
exit 0
