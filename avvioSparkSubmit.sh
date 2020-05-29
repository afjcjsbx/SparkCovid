docker run -it --name=spark-submit -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master --network=project_network richardsti/spark-submit
