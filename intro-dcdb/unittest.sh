#!/bin/bash
source common.sh
# app=../dcdb
cd $app

function unittest() {
    find . -name "*unittest.py" | xargs -i python {} > test.log 2>&1 &
    while ps -ef | grep "xargs -i python" | grep -v "grep"
    do
        sleep 1
        continue
    done
    grep -a -A 2 "Ran " test.log
}

echo -e "Section \033[1;31m $0 \033[0m start ..."
confirm_do 'find . -name "*unittest.py" -exec grep TestCase {} \;' "unittest-cases"
# confirm_do 'find . -name "*unittest.py" | xargs -i python {} > test.log 2>&1 &' "unittest"
confirm_do 'unittest' "unittest"
echo -e "Section \033[1;31m $0 \033[0m end ..."



# cd - > /dev/null
