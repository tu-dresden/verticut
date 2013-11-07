#!/bin/bash

cat ../config/redis.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    port=`echo "$LINE" | awk '{print $2}'`
    
    echo "Checking server $host..."
    ssh $host $" ps aux | pgrep redis-server >/dev/null && echo 'Normal, Memory usage %, CPU usage %: ' &&  ps aux | \
      grep redis-server | grep -v grep | grep -ve --server | awk '{print \$4; print \$3}' \
      || echo 'The server is down!'" &
    sleep 2 

done

wait
