#!/bin/bash
source common.sh
# app=../dcdb
cd $app

function benchdb() {
    bash stop.sh && sleep 1
    sed -i "s/DEBUG/ERROR/"  mylog.py
    cd test
    echo -e "\033[1;32m benchdb threads=1 table_size=1000 \033[0m"
    python benchdb.py
    cd -
    sed -i "s/ERROR/DEBUG/"  mylog.py
}

echo -e "Section \033[1;31m $0 \033[0m start ..."
confirm_do 'benchdb' "benchdb"
echo -e "Section \033[1;31m $0 \033[0m end ..."

