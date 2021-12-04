package com.redbeard.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Producer {

    public static FlinkKafkaProducer011<String> createProducer(String topic, String bootstrapServer) {
        return new FlinkKafkaProducer011<>(bootstrapServer, topic, new SimpleStringSchema());
    }
}
