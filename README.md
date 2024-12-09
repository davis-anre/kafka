# KAFKA

- https://hub.docker.com/r/apache/kafka
- Java 23

* (start DAULE) docker run -d --name broker apache/kafka:latest
* docker start brokerKafka
* (Stop) docker stop brokerKafka
* docker exec --workdir /opt/kafka/bin/ -it brokerKafka sh
  
* DURAN
  
  * ./kafka-topics.sh --create --bootstrap-server 192.168.1.4:9092 --replication-factor 1 --partitions 1 --topic duran-IN
  * ./kafka-topics.sh --create --bootstrap-server 192.168.1.4:9092 --replication-factor 1 --partitions 1 --topic duran-OUT
    
  * VER DATA
    
    * ./kafka-console-producer.sh --broker-list 192.168.1.4:9092 --topic duran-IN
    * ./kafka-console-consumer.sh --bootstrap-server 192.168.1.4:9092 --topic duran-OUT --from-beginning
      
* SAMBORONDON
  
  * ./kafka-topics.sh --create --bootstrap-server 192.168.1.4:9092 --replication-factor 1 --partitions 1 --topic samborondon-IN
  * ./kafka-topics.sh --create --bootstrap-server 192.168.1.4:9092 --replication-factor 1 --partitions 1 --topic samborondon-OUT
  * VER DATA
    
    * ./kafka-console-producer.sh --broker-list 192.168.1.4:9092 --topic samborondon-IN
    * ./kafka-console-consumer.sh --bootstrap-server 192.168.1.4:9092 --topic samborondon-OUT --from-beginning
      

* ./kafka-topics.sh --list --bootstrap-server 192.168.1.4:9092

# eliminar

* ./kafka-topics.sh --bootstrap-server 192.168.1.4:9092 --delete --topic duran-IN
* ./kafka-topics.sh --bootstrap-server 192.168.1.4:9092 --delete --topic duran-OUT
* ./kafka-topics.sh --bootstrap-server 192.168.1.4:9092 --delete --topic duran-IN
* ./kafka-topics.sh --bootstrap-server 192.168.1.4:9092 --delete --topic duran-OUT
