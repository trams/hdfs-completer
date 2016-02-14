

. $(dirname $0)/settings.sh

_complete_hdfs_path() {
    cur="${COMP_WORDS[COMP_CWORD]}"

    if [[ $cur == /* ]]; then
        possible_completetions="`curl --silent ${HDFS_COMPLETER_HOST}:${HDFS_COMPLETER_PORT}/v1/completetions?path=${cur}`"
        if test $? -eq 0; then
            COMPREPLY=($possible_completetions)
            return 0
        fi
    fi
}

complete -o nospace -F _complete_hdfs_path hdfs
