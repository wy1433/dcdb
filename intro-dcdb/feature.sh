#!/bin/bash
source common.sh
# app=../dcdb
cd $app

function crud() {
    bash restart.sh && sleep 1
    echo -e "\033[1;32m INSERT 5 rows into table \033[0m"
    echo "**********************************************************"
    echo "insert into student (id, name, age) values (-3, 'n3', 10);"
    echo "insert into student (id, name, age) values (-1, 'n1', 10);"
    echo "insert into student (id, name, age) values (0, 'n0', 5);"
    echo "insert into student (id, name, age) values (1, 'n1', 5);"
    echo "insert into student (id, name, age) values (3, 'n3', 10);"
    echo "**********************************************************"

    read -r -p "press any key to continue ..." input
    $mysql_cli "insert into student (id, name, age) values (-3, 'n3', 10);"
    $mysql_cli "insert into student (id, name, age) values (-1, 'n1', 10);"
    $mysql_cli "insert into student (id, name, age) values (0, 'n0', 5);"
    $mysql_cli "insert into student (id, name, age) values (1, 'n1', 5);"
    $mysql_cli "insert into student (id, name, age) values (3, 'n3', 10);"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m INSERT 1 rows into table with the dup key \033[0m"
    $mysql_cli "insert into student (id, name, age) values (3, 'n3', 10);"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m SELECT all \033[0m"
    $mysql_cli "SELECT id,name,age From student WHERE id > -10000;"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m SELECT, #3, #4 will be selected. \033[0m"
    $mysql_cli "SELECT id,name,age From student WHERE id > -2 AND id < 2 OR name = 'n3' AND age in (5 ,7);"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m DELETE #4 \033[0m"
    $mysql_cli "delete from student where id = 1;"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m SELECT again, #3 will be selected \033[0m"
    $mysql_cli "SELECT id,name,age From student WHERE id > -2 AND id < 2 OR name = 'n3' AND age in (5 ,7);"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m UPDATE #5 age  10 to 7 \033[0m"
    $mysql_cli "update student set age = 7 where id = 3;"
    read -r -p "press any key to continue ..." input

    echo -e "\033[1;32m SELECT again, #3, #5 will be selected \033[0m"
    $mysql_cli "SELECT id,name,age From student WHERE id > -2 AND id < 2 OR name = 'n3' AND age in (5 ,7);"
    read -r -p "press any key to continue ..." input
}

echo -e "Section \033[1;31m $0 \033[0m start ..."
confirm_do 'crud' "crud"
echo -e "Section \033[1;31m $0 \033[0m end ..."

