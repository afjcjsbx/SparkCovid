#!/bin/bash

docker exec master hdfs dfs -put fileSABD/prova.txt hdfs://0.0.0.0:54310/input.txt
docker exec master hdfs dfs -put fileSABD/time_series_covid19_confirmed_global.csv hdfs://0.0.0.0:54310/time_series_covid19_confirmed_global.csv
docker exec master hdfs dfs -put fileSABD/dpc-covid19-ita-andamento-nazionale.csv hdfs://0.0.0.0:54310/dpc-covid19-ita-andamento-nazionale.csv
