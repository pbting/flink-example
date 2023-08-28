package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/sources/
 */
public class FlinkKafkaSourceExample007 {

    public static void main(String[] args) throws Exception {

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("ifrat-demo")
                .setGroupId("ifra-cgroup")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

//        Properties properties = new Properties();
//        FlinkKafkaConsumer kafkaConsumer =
//                new FlinkKafkaConsumer("ifrat-demo", new SimpleStringSchema(), properties);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value + "-flink-map";
                    }
                }).addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        System.err.println(value);

                    }
                });

        environment.execute("kafka-example");
    }
}
