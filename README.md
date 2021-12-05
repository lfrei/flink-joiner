# flink-joiner

## Deployment

### Start

1. Build `flink-joiner` Project with `mvn clean package`
    - Creates a jar `flink-joiner-0.1-jar-with-dependencies.jar`
2. Start Kafka Broker
3. Create Topics (or allow auto creation):
    - `words-input`, `words-output`
    - `address`, `postal-code`, `delivery-address`
4. Download Flink [here](https://flink.apache.org/downloads.html)
5. Start Flink with `./bin/start-cluster.sh`
6. Navigate to Flink Web Interface at `http://localhost:8081`
7. Select "Submit New Job" and upload jar `flink-joiner-0.1-jar-with-dependencies.jar` 

### Stop

1. Shutdown Flink `./bin/stop-cluster.sh`
2. Shutdown Kafka Broker
