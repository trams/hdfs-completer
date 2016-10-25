#!/bin/bash

set -eu

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPT_ROOT="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

readonly CLUSTER_NAME=$1
readonly _PATH=$2

readonly SOCKET_PATH="/tmp/hdfs_completer/$CLUSTER_NAME"

if ! test -S $SOCKET_PATH; then
    echo >&2 "Hdfs completer server is down. Launching..."

    readonly CLUSTER_URL_FILE=$HOME/.hdfs_completer/clusters/$CLUSTER_NAME
    if ! test -f $CLUSTER_URL_FILE; then
        echo >&2 "File $CLUSTER_URL_FILE not found. Please populate it with a URL to $CLUSTER_NAME"
        exit 1
    fi

    CLUSTER_URL=$(cat $CLUSTER_URL_FILE)

    readonly LOG_PATH="/tmp/hdfs_completer/logs/$CLUSTER_NAME"
    mkdir -p /tmp/hdfs_completer/logs
    $SCRIPT_ROOT/env/bin/python $SCRIPT_ROOT/completer.py --hdfs_host=$CLUSTER_URL --logging=debug --use_kerberos --unix_socket=$SOCKET_PATH --log-file-prefix=$LOG_PATH &
    sleep 3
fi

curl --max-time 1 --silent --unix-socket $SOCKET_PATH http:/v1/list?path=$_PATH && echo
