#!/bin/bash
docker exec -d master mkdir fileSABD
docker cp files/prova.txt master:/fileSABD 
docker cp files/dpc-covid19-ita-andamento-nazionale.csv master:/fileSABD 
docker cp files/time_series_covid19_confirmed_global.csv master:/fileSABD

