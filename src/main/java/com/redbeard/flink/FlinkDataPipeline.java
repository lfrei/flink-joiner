package com.redbeard.flink;

import com.redbeard.flink.operator.ToUpperCase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.redbeard.flink.kafka.Source.createSource;
import static com.redbeard.flink.kafka.Sink.createSink;

public class FlinkDataPipeline {

    public static void capitalize(StreamExecutionEnvironment environment) {
        String inputTopic = "words-input";
        String outputTopic = "words-output";
        String consumerGroup = "words-capitalize";
        String bootstrapServer = "localhost:9092";

        KafkaSource<String> source = createSource(inputTopic, bootstrapServer, consumerGroup);
        KafkaSink<String> sink = createSink(outputTopic, bootstrapServer);

        environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "words-source")
                .map(new ToUpperCase())
                .sinkTo(sink);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        capitalize(environment);
        environment.execute();
    }
}
