
_complete_hdfs_path() {
    cur="${COMP_WORDS[COMP_CWORD]}"

    if [[ $cur == /* ]]; then
        if [ -f $HOME/.hdfs_completer/port ]; then
            PORT=$(cat ~/.hdfs_completer/port)
        else
            echo "Unimplemented"
        fi
        possible_completetions="`curl --max-time 1 --silent 127.0.0.1:${PORT}/v1/completetions?path=${cur}`"
        if test $? -eq 0; then
            COMPREPLY=($possible_completetions)
            return 0
        fi
    fi
}

complete -o nospace -F _complete_hdfs_path hdfs
