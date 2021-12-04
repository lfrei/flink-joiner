package com.redbeard.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class Consumer {

    public static FlinkKafkaConsumer011<String> createConsumer(String topic, String bootstrapServer, String kafkaGroup, boolean earliest) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServer);
        props.setProperty("group.id", kafkaGroup);

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        if (earliest) {
            consumer.setStartFromEarliest();
        }
        return consumer;
    }
}
