#!/bin/sh
#

CWD=$(dirname `readlink -f $0`)
cd ${CWD}
cd ..

PYTHON="./env/bin/python"

${PYTHON} scheduler.py

