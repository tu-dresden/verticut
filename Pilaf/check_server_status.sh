#!/bin/bash

cat ../config/pilaf.cnf| while read LINE
do
    ip=`echo "$LINE" | awk '{print $1}'`
    host="beaker-`echo $ip |awk '{split($1,s,".");print s[4];}'`"
    port=`echo "$LINE" | awk '{print $2}'`
    
    echo "Checking server $host..."
    ssh $host $" ps aux | pgrep dht-test >/dev/null && echo 'Normal, Memory usage %, CPU usage %: ' &&  ps aux | \
      grep dht-test | grep -v grep | grep -v server | awk '{print \$4; print \$3}' \
      || echo 'The server is down!'" &
    sleep 1 

done

wait
