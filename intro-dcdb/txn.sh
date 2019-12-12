#!/bin/bash
source common.sh
# app=../dcdb
cd $app

function txn() {
    bash restart.sh && sleep 1
    cd test/txndb

    echo -e "\033[1;32m DirtyRead \033[0m"
    read -r -p "press any key to continue ..." input
    vim -O DirtyRead.sh
    bash DirtyRead.sh
    read -r -p "press any key to continue ..." input
    vim -O DirtyRead?.sh
    read -r -p "press any key to continue ..." input
    vim -O DirtyRead*.log

    echo -e "\033[1;32m Non-Repeatable \033[0m"
    read -r -p "press any key to continue ..." input
    vim -O Non-Repeatable.sh
    bash Non-Repeatable.sh
    read -r -p "press any key to continue ..." input
    vim -O Non-Repeatable?.sh
    read -r -p "press any key to continue ..." input
    vim -O Non-Repeatable*.log
    
    echo -e "\033[1;32m Phantom \033[0m"
    read -r -p "press any key to continue ..." input
    vim -O Phantom.sh
    bash Phantom.sh
    read -r -p "press any key to continue ..." input
    vim -O Phantom?.sh
    read -r -p "press any key to continue ..." input
    vim -O Phantom*.log

    cd -
}

echo -e "Section \033[1;31m $0 \033[0m start ..."
confirm_do 'txn' "txn"
echo -e "Section \033[1;31m $0 \033[0m end ..."

