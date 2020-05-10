#!/bin/bash
docker exec -d master hdfs "$HADOOP_HOME/sbin/start-dfs.sh"

