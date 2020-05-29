docker run -it -p 7070:7070 -p 7077:7077 -p 4040:4040 --network=project_network --name=spark-master -e ENABLE_INIT_DAEMON=setup_spark -e SPARK_PUBLIC_DNS=127.0.0.1 -e HDFS_URL=hdfs://master:54310 afjcjsbx/spark

