package com.redbeard.flink;

import com.redbeard.flink.operator.AddressJoiner;
import com.redbeard.flink.operator.PostalCodeSelector;
import com.redbeard.flink.operator.ToUpperCase;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.redbeard.flink.kafka.Sink.createSink;
import static com.redbeard.flink.kafka.Source.createSource;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class FlinkDataPipeline {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void capitalize(StreamExecutionEnvironment environment) {
        KafkaSource<String> source = createSource("words-input", BOOTSTRAP_SERVER, "words-capitalize");
        KafkaSink<String> sink = createSink("words-output", BOOTSTRAP_SERVER);

        environment
                .fromSource(source, noWatermarks(), "words-source")
                .map(new ToUpperCase())
                .sinkTo(sink);
    }

    public static void joinAddress(StreamExecutionEnvironment environment) {
        KafkaSource<String> addressSource = createSource("address", BOOTSTRAP_SERVER, "joiner");
        KafkaSource<String> postalCodeSource = createSource("postal-code", BOOTSTRAP_SERVER, "joiner");
        KafkaSink<String> deliverySink = createSink("delivery-address", BOOTSTRAP_SERVER);

        DataStreamSource<String> addressStream = environment.fromSource(addressSource, noWatermarks(), "address-source");
        DataStreamSource<String> postalCodeStream = environment.fromSource(postalCodeSource, noWatermarks(), "postal-code-source");

        addressStream
                .join(postalCodeStream)
                .where(new PostalCodeSelector())
                .equalTo(new PostalCodeSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new AddressJoiner())
                .sinkTo(deliverySink);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        capitalize(environment);
        joinAddress(environment);
        environment.execute();
    }
}
