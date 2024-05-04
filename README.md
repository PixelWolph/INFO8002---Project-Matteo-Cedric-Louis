# Distributed-Computing-with-MapReduce-and-Spark

All information to run the project are on the PDF report. 

# See Debruyne github for docker information : https://github.com/chrdebru/docker-hadoop-spark

## Quick Start (Copy from Debruyne github)

To deploy an the HDFS-Spark cluster, run:
```
  docker-compose up
```

`docker-compose` creates a docker network that can be found by running `docker network list`, e.g. `docker-hadoop-spark`.

Run `docker network inspect` on the network (e.g. `docker-hadoop-spark`) to find the IP the hadoop interfaces are published on. Access these interfaces with the following URLs:

* Namenode: http://<dockerhadoop_IP_address>:9870/dfshealth.html#tab-overview
* History server: http://<dockerhadoop_IP_address>:8188/applicationhistory
* Datanode: http://<dockerhadoop_IP_address>:9864/
* Nodemanager: http://<dockerhadoop_IP_address>:8042/node
* Resource manager: http://<dockerhadoop_IP_address>:8088/
* Spark master: http://<dockerhadoop_IP_address>:8080/
* Spark worker 1: http://<dockerhadoop_IP_address>:8081/
* Spark worker 2: http://<dockerhadoop_IP_address>:8082/
* Spark worker 3: http://<dockerhadoop_IP_address>:8083/
