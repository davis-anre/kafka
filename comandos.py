def print_kafka_commands(bootstrap_server):
    """
    Print Kafka commands with the given bootstrap server.
    
    Args:
        bootstrap_server (str): Kafka bootstrap server address
    """
    print("\n\n> DURAN Topics ////////////////////")
    view(bootstrap_server, "duran-OUT", "duran-IN")

    print("\n\n> SAMBORONDON Topics  /////////////////////////////////////////////")
    view(bootstrap_server, "samborondon-OUT", " samborondon-IN")

    print("\n> List topics")
    print(f"./kafka-topics.sh --list --bootstrap-server {bootstrap_server}")
    print("\n\n\n\n\n")

def view(bootstrap_server, consumerTopic, producerTopic):
    print(f"./kafka-topics.sh --create --bootstrap-server {bootstrap_server} --replication-factor 1 --partitions 1000 --topic {producerTopic}")
    print(f"./kafka-topics.sh --create --bootstrap-server {bootstrap_server} --replication-factor 1 --partitions 1000 --topic {consumerTopic}")
    print(f"\n>>> Data Viewing -------------------------------------------------")
    print(f"./kafka-console-consumer.sh --bootstrap-server {bootstrap_server} --topic {consumerTopic} --from-beginning")
    print("\n> Ver todas las particiones")
    print(f"./kafka-topics.sh --bootstrap-server {bootstrap_server} --describe --topic {consumerTopic}")
    print("\n> Ver la data de una particion, solo se pone el numero de la particion")
    print(f"./kafka-console-consumer.sh --bootstrap-server {bootstrap_server} --topic {consumerTopic} --partition <nparticion> --offset 0 --from-beginning")
    print("\n> Ver la data de una particion con su key y data")
    print(f"./kafka-console-consumer.sh --bootstrap-server {bootstrap_server} --topic {consumerTopic} --partition <nparticion> --from-beginning --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer  --from-beginning")
    print("\n> Ver todas las particion en con su key y value que se añade")
    print(f"./kafka-console-consumer.sh --bootstrap-server {bootstrap_server} --topic {consumerTopic} --from-beginning --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --from-beginning")


def main():
    import sys
    
    # Check if bootstrap server is provided
    if len(sys.argv) < 2:
        print("Please provide the Kafka bootstrap server address.")
        print("Usage: python script.py <bootstrap_server>")
        print("Example: python script.py localhost:9092 or 192.168.1.7:9092")
        sys.exit(1)
    
    # Get the bootstrap server from command-line argument
    bootstrap_server = sys.argv[1]
    
    # Print the Kafka commands
    print_kafka_commands(bootstrap_server)

if __name__ == "__main__":
    main()