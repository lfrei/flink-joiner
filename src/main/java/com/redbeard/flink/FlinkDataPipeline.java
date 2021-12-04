package com.redbeard.flink;

import com.redbeard.flink.operator.ToUpperCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import static com.redbeard.flink.kafka.Consumer.createConsumer;
import static com.redbeard.flink.kafka.Producer.createProducer;

public class FlinkDataPipeline {

    public static void capitalize(StreamExecutionEnvironment environment) {
        String inputTopic = "words-input";
        String outputTopic = "words-output";
        String consumerGroup = "words-capitalize";
        String bootstrapServer = "localhost:9092";

        FlinkKafkaConsumer011<String> consumer = createConsumer(inputTopic, bootstrapServer, consumerGroup, true);
        FlinkKafkaProducer011<String> producer = createProducer(outputTopic, bootstrapServer);

        environment
                .addSource(consumer)
                .map(new ToUpperCase())
                .addSink(producer);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        capitalize(environment);
        environment.execute();
    }
}
