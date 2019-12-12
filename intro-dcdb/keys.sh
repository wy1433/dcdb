#!/bin/bash
source common.sh
# app=../dcdb
cd $app

function dynamic_column() {
    echo -e "\033[1;32m init table with 2 columns \033[0m"
    sed -i "s/ ColumnInfo(3, 'age'/#ColumnInfo(3, 'age'/"  meta/config.py
    bash restart.sh
    sleep 1
    cat meta/config.py
    ls -l $app/data/store/
    $client "insert into student (id, name) values (1, 'name1');"
    read -r -p "press any key to continue ..." input
    $client "insert into student (id, name) values (2, 'name2');"
    read -r -p "press any key to continue ..." input
    $client "select id, name from student where id > 0;"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m alter table with 3 columns dynamic \033[0m"
    sed -i "s/#ColumnInfo(3, 'age'/ ColumnInfo(3, 'age'/"  meta/config.py
    bash restart.sh --noclean
    sleep 1
    cat meta/config.py
    ls -l $app/data/store/
    $client "insert into student (id, name, age) values (3, "name3", 10);"
    read -r -p "press any key to continue ..." input
    $client "select id, name, age from student where id > 0;"
    read -r -p "press any key to continue ..." input
}

echo -e "Section \033[1;31m $0 \033[0m start ..."
confirm_do 'dynamic_column' "dynamic_column"
echo -e "Section \033[1;31m $0 \033[0m end ..."



# cd - > /dev/null
