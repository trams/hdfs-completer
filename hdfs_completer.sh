

_complete_hdfs_path() {
    cur="${COMP_WORDS[COMP_CWORD]}"

    if [[ $cur == /* ]]; then
        possible_completetions="`curl --silent 127.0.0.1:8888/v1/completetions?path=${cur}`"
        possible_completetions=`echo "$possible_completetions" | sed -e 's/ *$/\//'`
        if test $? -eq 0; then
            COMPREPLY=($possible_completetions)
            return 0
        fi
    fi
}

complete -o nospace -F _complete_hdfs_path hdfs
