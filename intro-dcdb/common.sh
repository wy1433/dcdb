#!/bin/bash
app=~/code/dcdb
client=$app/test/dcdb-cli
mysql_cli=$app/test/mysql_cli
export DEBUG=true

function confirm_do {
    local __cmd=$1
    local __desc=$2
    # echo ${__cmd}
    read -r -p "${__desc}? [Y/n] " input
    if [ "$input" != "n" ]
    then
        eval ${__cmd} 
    fi
}

function DEBUG() {
    if [ "$DEBUG" = "true" ]; then
        $@
    fi
}

