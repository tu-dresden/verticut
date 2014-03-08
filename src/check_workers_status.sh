#!/bin/bash

cat ../config/workers.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    
    echo "Checking server $host..."
    ssh $host $" ps aux | grep distributed-image-search | grep -v grep | grep -v python" & 
    sleep 1 

done

wait
