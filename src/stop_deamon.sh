#!/bin/bash

cat ../config/workers.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    
    echo "Stop deamon on $host..."
    ssh $host $"pkill bitmap-deamon"&
    sleep 2 

done

wait
