#!/usr/bin/env bash

set -o nounset   # Treat unset variables as an error
set -o errexit   # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Fail if any command in a pipeline fails

function envsubst_tpl() {
    local tpl_path=$1
    local basename=$(basename "$tpl_path")

    if [[ $basename != tpl-* ]]; then
        echo "Error: Invalid template path. Path must contain 'tpl-' prefix."
        return 1
    fi
    

    local dir=$(dirname "$tpl_path")
    local dest_path="$dir/${basename#tpl-}"
    echo "subs $basename"
    envsubst < "$tpl_path" > "$dest_path"
}

envsubst_tpl "$1"