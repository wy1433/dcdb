#!/bin/bash
./dcdb-client a "begin"  
sleep 1
./dcdb-client a "select name from student where name > 'B0';"  
sleep 1
./dcdb-client a "insert into student (id, name, age) values (3, 'B2', 30);"  
sleep 1
./dcdb-client a "commit"  
sleep 1
./dcdb-client a "select name from student where name > 'B0';"  
