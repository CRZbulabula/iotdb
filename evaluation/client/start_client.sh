#!/bin/bash

if [ "$1" == "1" ]; then
    echo "1121"
    nohup java -jar workload-simulate-jar-with-dependencies.jar -cp . --config config_1121.txt --ip 172.21.32.5 --client-count 15 --load-csv --monitor "$@" > client.log &
elif [ "$1" == "2" ]; then
    echo "1204"
    nohup java -jar workload-simulate-jar-with-dependencies.jar -cp . --config config_1204.txt --ip 172.21.32.5 --client-count 15 --load-csv --monitor "$@" > client.log &
else
    echo "The first argument is neither 1 nor 2"
    exit
fi