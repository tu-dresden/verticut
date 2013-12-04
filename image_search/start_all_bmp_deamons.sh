#!/bin/bash

cat ../config/workers.cnf| while read LINE
do
    host=`echo "$LINE" | awk '{print $1}'`
    
    ssh $host $"cd workplace/image_search/image_search; nohup ./bitmap-deamon &>/dev/null &" &

    sleep 1 
    echo "Deamon $host started"
done

wait
