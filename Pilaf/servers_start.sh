#!/bin/bash

if [ $# -eq 1 ]
then
	PRESIZE="P"
else
	PRESIZE=0
fi

./dht-test -s 36001 $PRESIZE </dev/null
#nohup ./dht-test -s 4003 $PRESIZE </dev/null >/dev/null 2>/dev/null &
#nohup ./dht-test -s 4004 $PRESIZE </dev/null >/dev/null 2>/dev/null &
#nohup ./dht-test -s 4005 $PRESIZE </dev/null >/dev/null 2>/dev/null &
#sleep 10
echo "Servers started"

exit 0
