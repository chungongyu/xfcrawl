#!/bin/sh
#

CWD=$(dirname `readlink -f $0`)
cd ${CWD}
cd ..

PYTHON="./env/bin/python"

AMQP_SRV="amqp://localhost:5672"
REDIS="redis://localhost:6379"
LOG_CONF="config/log4j.properties"

nohup watch -n 300 ${PYTHON} downloader.py -s ${AMQP_SRV} -r ${REDIS} -c ${LOG_CONF} > /dev/null 2>&1&

