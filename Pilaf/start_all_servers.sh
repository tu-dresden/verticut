#!/bin/bash

if [ $# -eq 1 ]
then
	PRESIZE="P"
else
	PRESIZE=0
fi

cat dht-test.cnf| while read LINE
do
    ip=`echo "$LINE" | awk '{print $1}'`
    host="beaker-`echo $ip |awk '{split($1,s,".");print s[4];}'`"
    port=`echo "$LINE" | awk '{print $2}'`
    ssh $host $"cd workplace/image_search/Pilaf; nohup ./dht-test -s $port $PRESIZE </dev/null &>/dev/null &" &
    sleep 5
    echo "Server $host:$port started"
done

wait
