# SparkCovid
** First project of SABD A.A. 2019/2020 **

## Configuration

### N.B. This configuration is tested on Ubuntu os.

### To start the project:
Open a terminal in the main directory of the project and exec the command
```
sudo docker-compose up
```

It will begin to pull the docker images used. We have used docker images mostly revisited by us present in our docker hub.

When Docker compose has started the master, open a new terminal and type:

```
sudo docker exec -it master /bin/bash
```

And then, in the same terminal type

```
sh data/startHDFS.sh
```

This command will format the HDFS and run it.
From now on you can open on your browser a new web page and type [http://localhost:9870/](http://localhost:9870/) to see the status of the HDFS from webUI.

To see the NiFi status open a new web page on your browser and type [http://localhost:8080/nifi](http://localhost:8080/nifi)

Now, after Nifi and HDFS have been started, the Spark master will automatically start.

Finally the results of the executions will be available in the HDFS, in the "results" folder and in Redis at the address link [http://localhost:5001](http://localhost:5001).

By opening the Redis web page, type `redis` in place of localhost, next to the `host` entry.
Selecting the database ID, with ID = 1 we will go to view the result of query1, with ID = 2 we will go to view the result of query 2, with ID = 3 we will go to view the result of query 3 with Spark MLlib and with ID = 4 we will go to view the result of query 3 with K-means naive.

### Kill and remove
To kill all the containers, type `CTRL + C` on the terminal where the `sudo docker-compose up` command was launched and finally to remove them, run the command

```
sh stopAndClean.sh
```
