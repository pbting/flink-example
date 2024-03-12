package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class FlinkDynamicKafkaTopicSourceExample020 {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), String.valueOf(TimeUnit.SECONDS.toMillis(3)));

        DataStreamSource<String> clsSource =
                environment.fromSource(buildKafkaSource(kafkaProperties), WatermarkStrategy.noWatermarks(), "cls-topic");

        clsSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "dynamic-topic-" + value;
            }
        }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.err.println("sink: " + value);
            }
        });

        environment.execute("dynamic-topic");
    }

    public static KafkaSource<String> buildKafkaSource(Properties kafkaProperties) {

        return KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopicPattern(Pattern.compile("^quickstart.*"))
                .setGroupId("cls-cgroup")
                .setProperties(kafkaProperties)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaRecordDeserializationSchema<String>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord,
                                            Collector<String> collector) throws IOException {

                        byte[] value = consumerRecord.value();

                        collector.collect(new String(value));
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
    }
}
