#!/bin/bash
cd ../..
bash restart.sh
sleep 2
cd -

./dcdb-client login

./dcdb-client i "insert into student (id, name, age) values (1, 'B0', 10);"
./dcdb-client i "insert into student (id, name, age) values (2, 'B1', 20);"

bash PhantomA.sh > PhantomA.log 2>/dev/null &
bash PhantomB.sh > PhantomB.log 2>/dev/null &
