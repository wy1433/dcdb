#!/bin/bash
sleep 1
./dcdb-client b "begin"  
sleep 1
./dcdb-client b "select name from student where id = 1;"  
sleep 1
./dcdb-client b "select name from student where id = 1;"  
sleep 1
./dcdb-client b "commit"  
