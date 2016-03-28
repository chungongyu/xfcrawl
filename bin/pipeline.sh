#!/bin/sh
#

CWD=$(dirname `readlink -f $0`)
cd ${CWD}
cd ..

PYTHON="./env/bin/python"

AMQP_SRV="amqp://localhost:5672"
LOG_CONF="config/log4j.properties"

watch -n 300 ${PYTHON} pipeline.py -s ${AMQP_SRV} -c ${LOG_CONF} > /dev/null 2>&1 &
