#!/bin/bash
sleep 1
./dcdb-client b "begin"  
sleep 1
./dcdb-client b "select name from student where name > 'B0';"  
sleep 1
./dcdb-client b "select name from student where name > 'B0';"  
sleep 1
./dcdb-client b "commit"  
