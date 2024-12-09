def print_kafka_commands(bootstrap_server):
    """
    Print Kafka commands with the given bootstrap server.
    
    Args:
        bootstrap_server (str): Kafka bootstrap server address
    """
    print("\n> DURAN Topics")
    print(f"./kafka-topics.sh --create --bootstrap-server {bootstrap_server} --replication-factor 1 --partitions 1 --topic duran-IN")
    print(f"./kafka-topics.sh --create --bootstrap-server {bootstrap_server} --replication-factor 1 --partitions 1 --topic duran-OUT")
    
    print("\n> DURAN Data Viewing")
    print(f"./kafka-console-consumer.sh --bootstrap-server {bootstrap_server} --topic duran-OUT --from-beginning")
    
    print("\n> SAMBORONDON Topics")
    print(f"./kafka-topics.sh --create --bootstrap-server {bootstrap_server} --replication-factor 1 --partitions 1 --topic samborondon-IN")
    print(f"./kafka-topics.sh --create --bootstrap-server {bootstrap_server} --replication-factor 1 --partitions 1 --topic samborondon-OUT")
    
    print("\n> SAMBORONDON Data Viewing")
    print(f"./kafka-console-consumer.sh --bootstrap-server {bootstrap_server} --topic samborondon-OUT --from-beginning")
    
    print("\n> List topics")
    print(f"./kafka-topics.sh --list --bootstrap-server {bootstrap_server}")

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