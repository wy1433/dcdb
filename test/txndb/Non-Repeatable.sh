#!/bin/bash
cd ../..
bash restart.sh
sleep 2
cd -

./dcdb-client login

./dcdb-client i "insert into student (id, name, age) values (1, 'A0', 10);"

bash Non-RepeatableA.sh > Non-RepeatableA.log 2>/dev/null &
bash Non-RepeatableB.sh > Non-RepeatableB.log 2>/dev/null &
