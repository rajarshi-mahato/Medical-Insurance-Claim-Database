#!/bin/bash
HDFS_ROOT="$HOME/med_claims_d/hdfs_cluster"
mkdir -p "$HDFS_ROOT"

if [ "$1" == "dfs" ]; then
    if [ "$2" == "-mkdir" ]; then 
        mkdir -p "$HDFS_ROOT$3"
        echo "HDFS: Created directory at $3"
    elif [ "$2" == "-put" ]; then 
        cp "$3" "$HDFS_ROOT$4"
        echo "HDFS: Uploaded $(basename $3) to $4"
    elif [ "$2" == "-ls" ]; then 
        echo "Found items in HDFS $3:"
        ls -lh "$HDFS_ROOT$3"
    else 
        echo "HDFS: Command not recognized."
    fi
else
    echo "Usage: hdfs dfs [-mkdir | -put | -ls]"
fi
