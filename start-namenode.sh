#!/bin/bash
set -e

# Format NameNode only if needed
if [ ! -d /hadoop/dfs/name/current ]; then
    hdfs namenode -format -force -nonInteractive
fi

# Start Hadoop DFS & YARN daemons
start-dfs.sh
start-yarn.sh

# Create folder & upload stopwords.txt
hdfs dfs -mkdir -p /user/student/needed
hdfs dfs -put -f /tmp/stopwords.txt /user/student/needed/

# Keep container running
tail -f /dev/null