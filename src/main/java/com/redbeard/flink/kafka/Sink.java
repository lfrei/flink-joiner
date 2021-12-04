package com.redbeard.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class Sink {

    public static KafkaSink<String> createSink(String topic, String bootstrapServer) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setRecordSerializer(createStringRecordSerializer(topic))
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static KafkaRecordSerializationSchema<String> createStringRecordSerializer(String topic) {
        return KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();
    }
}
