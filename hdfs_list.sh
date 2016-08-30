#!/bin/bash

. $(dirname $0)/settings.sh

curl --max-time 1 ${HDFS_COMPLETER_HOST}:${HDFS_COMPLETER_PORT}/v1/list?path=$1
echo
