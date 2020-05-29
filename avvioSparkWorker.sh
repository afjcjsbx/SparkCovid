docker run -it --network=project_network --name spark-worker-1 --link spark-master:spark-master -e ENABLE_INIT_DAEMON=false bde2020/spark-worker:2.4.5-hadoop2.7
