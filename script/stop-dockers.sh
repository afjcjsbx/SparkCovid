#!/bin/bash

#STOP CONTAINERS
docker kill master slave1 slave2 slave3
docker rm master slave1 slave2 slave3
docker network rm hadoop_network
