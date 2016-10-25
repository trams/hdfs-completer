
_complete_hdfs_path_unix() {
    local cur="${COMP_WORDS[COMP_CWORD]}"

    if [[ ${COMP_CWORD} == 1 ]] ; then
        possible_completetions=$(ls /tmp/hdfs_completer)
        COMPREPLY=( $(compgen -W "${possible_completetions}" -- ${cur}) )
        return 0
    fi

    local cluster="${COMP_WORDS[COMP_CWORD-1]}"
    local socket_path="/tmp/hdfs_completer/$cluster"

    if [[ $cur == /* ]]; then
        possible_completetions="`curl --max-time 1 --unix-socket $socket_path --silent http:/v1/completetions?path=${cur}`"
        if test $? -eq 0; then
            COMPREPLY=($possible_completetions)
            return 0
        fi
        return 1
    fi
}

complete -o nospace -F _complete_hdfs_path_unix hdfsls
