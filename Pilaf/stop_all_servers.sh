#!/bin/bash

cat ../config/pilaf_1b.cnf| while read LINE
do
    ip=`echo "$LINE" | awk '{print $1}'`
    host="beaker-`echo $ip |awk '{split($1,s,".");print s[4];}'`"
    port=`echo "$LINE" | awk '{print $2}'`
    ssh $host "killall dht-test" &
    echo "Server $host:$port stoped"
done

wait
