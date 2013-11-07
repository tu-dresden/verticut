#!/bin/bash

cat ../config/redis.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    port=`echo "$LINE" | awk '{print $2}'`
    ssh $host "killall redis-server" &
    echo "Server $host:$port stoped"
done

wait
