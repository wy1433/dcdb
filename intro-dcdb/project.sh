#!/bin/bash
source common.sh
# app=../dcdb
cd $app

echo -e "Section \033[1;31m $0 \033[0m start ..."
confirm_do "cloc ." "code statics"
confirm_do 'tree -L 2 -I "*.pyc"' "code structure"
echo -e "Section \033[1;31m $0 \033[0m end ..."

# cd - > /dev/null
