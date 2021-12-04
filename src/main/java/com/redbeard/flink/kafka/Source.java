package com.redbeard.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class Source {

    public static KafkaSource<String> createSource(String topic, String bootstrapServer, String kafkaGroup) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(topic)
                .setGroupId(kafkaGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
