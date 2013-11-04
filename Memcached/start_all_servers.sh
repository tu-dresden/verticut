#!/bin/bash

cat ../config/memcached.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    port=`echo "$LINE" | awk '{print $2}'`

    if [[ -z "$port" ]]; then
      ssh $host $" nohup memcached -m 30096 -M &>/dev/null &" &
    else
      ssh $host $" nohup memcached -m 30096 -M -p $port &>/dev/null &" &
    fi

    sleep 1 
    echo "Server $host:$port started"
done

wait
