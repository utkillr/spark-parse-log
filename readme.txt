1. HDFS
	cd docker-hdfs
	docker build -t hdfs:custom .
	docker run -d -p 8088:8088 -p 9000:9000 -p 50070:50070 -p 50075:50075 -p 50030:50030 -p 50060:50060 --name hdfs hdfs:custom
	docker exec -it hdfs bash
		# idk maybe gunzip is included to gzip
		apt-get install wget gzip gunzip
		wget <file link>
		gunzip <file name>
		hadoop fs -mkdir /nasa
		hadoop fs -put <file name> /nasa/<file name>
		### OR run 'docker cp <file_on_host> <file_on_docker> and then only do hadoop cmds

2. Spark
2.0 Spark base
	cd docker-spark/base
	docker build -t spark-base:custom
2.1 Spark master
	cd docker-spark/master
	docker build -t spark-master:custom .
	docker run -d -p 8080:8080 -p 7077:7077 --name spark-master spark-master:custom
2.2 Spark worker
	cd docker-spark/worker
	vi Dockerfile # change 'spark://<address>:7077' to 'your dockers host ip' (maybe localhost or 0.0.0.0 works too, I didn't check - default was 'spark-master', maybe container name can be discovered via docker dhcp or something like this)
	docker build -t spark-worker:custom .
	docker run -d -p 8081:8081 --name spark-worker spark-worker:custom

3. Local part
	Install SPARK - https://spark.apache.org/downloads.html
	Setup SPARK - https://dzone.com/articles/working-on-apache-spark-on-windows
	spark-shell --master spark://<your dockers host ip>:7077
	...run there...
	
	