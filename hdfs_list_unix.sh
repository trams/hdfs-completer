#!/bin/bash

set -eu

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")

readonly CLUSTER_NAME=$1
readonly _PATH=$2

readonly SOCKET_PATH="/tmp/hdfs_completer/$CLUSTER_NAME"

if ! test -S $SOCKET_PATH; then
    echo >&2 "Hdfs completer server is down. Launching..."

    CLUSTER_URL=$(cat $HOME/.hdfs_completer/clusters/$CLUSTER_NAME)
    readonly LOG_PATH="/tmp/hdfs_completer/logs/$CLUSTER_NAME"
    mkdir -p /tmp/hdfs_completer/logs
    $SCRIPT_ROOT/env/bin/python ./completer.py --hdfs_host=$CLUSTER_URL --logging=debug --use_kerberos --unix_socket=$SOCKET_PATH --log-file-prefix=$LOG_PATH &
    sleep 3
fi

curl --max-time 1 --silent --unix-socket $SOCKET_PATH http:/v1/list?path=$_PATH && echo
