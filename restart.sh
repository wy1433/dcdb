#!/bin/bash
pid=`ps -ef | grep "web.py" |grep -v "grep" | awk '{print $2}'`
if [ "$pid" ]; then
    echo "stop: "$pid
    kill -9 $pid
fi

if [ -f log.txt ];then
    rm log.txt
fi

if [ $# -eq 1 -a "$1" == "--noclean" ];then
    :
    # echo "noclean"
else
    rm -rf data/meta/*
    rm -rf data/store/*
    # echo "clean"
fi

python web.py > log.txt 2>&1 &

pid=`ps -ef | grep "web.py" |grep -v "grep" | awk '{print $2}'`
while [ -z $pid ]
do
    sleep 1
    pid=`ps -ef | grep "web.py" |grep -v "grep" | awk '{print $2}'`
done
echo "start: "$pid
