#!/bin/bash
cd ../..
bash restart.sh
sleep 2
cd -

./dcdb-client login

./dcdb-client i "insert into student (id, name, age) values (1, 'A0', 10);"

bash DirtyReadA.sh > DirtyReadA.log 2>/dev/null &
bash DirtyReadB.sh > DirtyReadB.log 2>/dev/null &
