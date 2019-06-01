#!/bin/bash
pid=`ps -ef | grep "web.py" |grep -v "grep" | awk '{print $2}'`
if [ "$pid" ]; then
    echo "stop: "$pid
    kill -9 $pid
fi

rm log.txt

rm -rf data/meta/*
rm -rf data/store/*

python web.py > log.txt 2>&1 &

pid=`ps -ef | grep "web.py" |grep -v "grep" | awk '{print $2}'`
echo "start: "$pid
