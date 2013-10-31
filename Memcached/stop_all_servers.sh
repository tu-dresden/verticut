#!/bin/bash

cat ../config/memcached.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    port=`echo "$LINE" | awk '{print $2}'`
    ssh $host "killall memcached" &
    echo "Server $host:$port stoped"
done

wait
