#!/bin/bash

echo -e "\033[1;31m dcdb intro \033[0m start ..."
# cat << EOF
# **********************************
#     the following will be show:
# **********************************
#     1) overview
#     2) project
#     3) unittest
#     4) keys
#     5) feature
#     6) beachmark
#     7) txn
# cd $app
# EOF
bash overview.sh
bash project.sh
bash unittest.sh
bash keys.sh
bash feature.sh
bash beachmark.sh
bash txn.sh
echo -e "\033[1;31m dcdb intro \033[0m end ..."

