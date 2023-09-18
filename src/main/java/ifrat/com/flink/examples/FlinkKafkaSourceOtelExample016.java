package ifrat.com.flink.examples;

import ifrat.com.flink.examples.otel.ExportMetricsServiceRequestUtil;
import ifrat.com.flink.examples.otel.UnionMetricAggResult;
import ifrat.com.flink.examples.otel.UnionMetricGroupByModel;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

// 必须要加这一行，否则反序列化失败，对应的 maven 依赖是:

/**
 * <pre>
 *      <dependency>
 *             <groupId>com.twitter</groupId>
 *             <artifactId>chill-protobuf</artifactId>
 *             <version>0.10.0</version>
 *             <exclusions>
 *                 <exclusion>
 *                     <groupId>com.esotericsoftware.kryo</groupId>
 *                     <artifactId>kryo</artifactId>
 *                 </exclusion>
 *             </exclusions>
 *         </dependency>
 * </pre>
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/sources/
 */
public class FlinkKafkaSourceOtelExample016 {

    public static void main(String[] args) throws Exception {

        KafkaSource<ExportMetricsServiceRequest> kafkaSource = KafkaSource.<ExportMetricsServiceRequest>builder()
                .setBootstrapServers("10.253.17.30:29094")
                .setTopics("skywalking-otel-meters")
                .setGroupId("cls-cgroup")
                .setProperty("partition.discovery.interval.ms", "10000")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaRecordDeserializationSchema<ExportMetricsServiceRequest>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord,
                                            Collector<ExportMetricsServiceRequest> collector) throws IOException {

                        byte[] value = consumerRecord.value();
                        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(value);

                        collector.collect(request);
                    }

                    @Override
                    public TypeInformation<ExportMetricsServiceRequest> getProducedType() {
                        return TypeInformation.of(ExportMetricsServiceRequest.class);
                    }
                })
                .build();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().registerTypeWithKryoSerializer(ExportMetricsServiceRequest.class,
                com.twitter.chill.protobuf.ProtobufSerializer.class);
        environment.getConfig().setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(3));
        environment.fromSource(kafkaSource, OtelWaterMarkUtil.otelWatermark(), "kafka-source")
                .setParallelism(1)
                .name("otel-kafka")
                .flatMap(new RichFlatMapFunction<ExportMetricsServiceRequest, UnionMetricGroupByModel>() {

                    /**
                     * 将哪些个指标进行分组计算 A+B、C+D、A+C => A、B、C、D 四个指标发送到同一个算子
                     * 例如下面这个例子 // sw_jvm.cpu.usage and sw_jvm.thread.live.count 这两个指标
                     * @param value The input value.
                     * @param out   The collector for returning result values.
                     * @throws Exception
                     */
                    final List<String> unionMetricKey = Arrays.asList("sw_jvm.cpu.usage", "sw_jvm.thread.live.count");
                    // 暂时先写死一个固定的，到时候生产生是动态指定的
                    final String unionGroupKey = StringUtils.join(unionMetricKey, "#");

                    @Override
                    public void flatMap(ExportMetricsServiceRequest value, Collector<UnionMetricGroupByModel> out) throws Exception {
                        List<UnionMetricGroupByModel> groupByModels = ExportMetricsServiceRequestUtil.extractMetric(value);

                        // 往下游发的时候，指定 key by
                        for (UnionMetricGroupByModel model : groupByModels) {
                            String groupKey = unionMetricKey.contains(model.getName()) ? unionGroupKey : model.getName();
                            model.setGroupKey(groupKey);
                            out.collect(model);
                        }
                    }
                })
                .keyBy(new KeySelector<UnionMetricGroupByModel, String>() {
                    @Override
                    public String getKey(UnionMetricGroupByModel value) throws Exception {
                        return value.getGroupKey();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<UnionMetricGroupByModel, UnionMetricAggResult, UnionMetricAggResult>() {
                    @Override
                    public UnionMetricAggResult createAccumulator() {
                        return new UnionMetricAggResult();
                    }

                    /**
                     * 注意: 这里面有可能多个联合指标备份到同一个组
                     * @param value The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public UnionMetricAggResult add(UnionMetricGroupByModel value, UnionMetricAggResult accumulator) {

                        if (accumulator.getGroupKey() == null) {
                            accumulator.setGroupKey(value.getGroupKey());
                            accumulator.setTimestamp(value.getTimestamp());
                        }

                        // 举个例子，这里对每个指标统计 min、max 等值
                        // 1. 计算 min
                        long currentVal = value.getValue();
                        if (currentVal < accumulator.getMinVal()) {
                            accumulator.setMinVal(currentVal);
                        }

                        // 2. 计算 max
                        if (currentVal > accumulator.getMaxVal()) {
                            accumulator.setMaxVal(currentVal);
                        }

                        // 3. 计算 sum, 同时统计 count，以便后面计算 avg
                        accumulator.setCount(accumulator.getCount() + 1);
                        accumulator.setSum(accumulator.getSum() + currentVal);

                        return accumulator;
                    }

                    @Override
                    public UnionMetricAggResult getResult(UnionMetricAggResult accumulator) {

                        accumulator.setAvg((double) accumulator.getSum() / accumulator.getCount());

                        return accumulator;
                    }

                    @Override
                    public UnionMetricAggResult merge(UnionMetricAggResult a, UnionMetricAggResult b) {
                        return null;
                    }
                })
                .addSink(new SinkFunction<UnionMetricAggResult>() {
                    @Override
                    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
                        if (value.getGroupKey().contains("#")) {
                            System.err.println(value);
                        }
                    }
                })
        ;

        environment.execute("kafka-otel-meters-example");
    }
}
