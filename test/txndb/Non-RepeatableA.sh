#!/bin/bash
./dcdb-client a "begin"  
sleep 1
./dcdb-client a "select name from student where id = 1;"  
sleep 1
./dcdb-client a "update student set name = 'A1' where id = 1;"  
sleep 1
./dcdb-client a "commit"  
sleep 1
./dcdb-client a "select name from student where id = 1;"  
