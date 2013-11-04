#!/bin/bash

cat ../config/memcached.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    port=`echo "$LINE" | awk '{print $2}'`
    
    echo "Checking server $host..."
    ssh $host $" ps aux | pgrep memcached >/dev/null && echo 'Normal, Memory usage %, CPU usage %: ' &&  ps aux | \
      grep memcached | grep -v grep | grep -v server | awk '{print \$4; print \$3}' \
      || echo 'The server is down!'" &
    sleep 1 

done

wait
