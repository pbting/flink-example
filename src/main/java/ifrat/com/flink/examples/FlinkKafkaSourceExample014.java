package ifrat.com.flink.examples;

import ifrat.com.flink.examples.otel.ExportMetricsServiceRequestUtil;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/sources/
 */
public class FlinkKafkaSourceExample014 {

    public static void main(String[] args) throws Exception {

        KafkaSource<ExportMetricsServiceRequest> kafkaSource = KafkaSource.<ExportMetricsServiceRequest>builder()
                .setBootstrapServers("10.253.17.30:39094")
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
         */
        environment.getConfig().registerTypeWithKryoSerializer(ExportMetricsServiceRequest.class,
                com.twitter.chill.protobuf.ProtobufSerializer.class);
        environment.getConfig().setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(3));
        environment.fromSource(kafkaSource,
                        WatermarkStrategy.<ExportMetricsServiceRequest>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ExportMetricsServiceRequest>() {
                                    @Override
                                    public long extractTimestamp(ExportMetricsServiceRequest element, long recordTimestamp) {
                                        return ExportMetricsServiceRequestUtil.extractTimestamp(element);
                                    }
                                }),
                        "kafka-source")
                // 注意: 这里的并行度要和 topic 的 分区数保持一致，不然就会导致窗口一直不触发，参考:
                // https://blog.csdn.net/weixin_43956734/article/details/120252842?spm=1001.2101.3001.6661.1&utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-1-120252842-blog-123002816.235%5Ev38%5Epc_relevant_sort&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-1-120252842-blog-123002816.235%5Ev38%5Epc_relevant_sort&utm_relevant_index=1
                .setParallelism(1)
                .name("otel-kafka")
                .flatMap(new RichFlatMapFunction<ExportMetricsServiceRequest, GroupMetricsModel>() {

                    /**
                     * 将哪些个指标进行分组计算
                     *
                     * @param value The input value.
                     * @param out   The collector for returning result values.
                     * @throws Exception
                     */
                    @Override
                    public void flatMap(ExportMetricsServiceRequest value, Collector<GroupMetricsModel> out) throws Exception {
                        List<ResourceMetrics> resourceMetrics = value.getResourceMetricsList();
                        final HashMap<String, Metric> metricMap = new HashMap<>();
                        final long timestamp = ExportMetricsServiceRequestUtil.extractTimestamp(value);
                        System.out.println("=> " + new Date(timestamp));
                        // sw_jvm.cpu.usage and sw_jvm.thread.live.count 这两个指标
                        for (ResourceMetrics metrics : resourceMetrics) {
                            List<ScopeMetrics> scopeMetrics = metrics.getScopeMetricsList();
                            for (ScopeMetrics scopeMetric : scopeMetrics) {
                                if (scopeMetric.getMetricsCount() > 0) {
                                    for (Metric metric : scopeMetric.getMetricsList()) {
                                        metricMap.put(metric.getName(), metric);
                                    }
                                }
                            }
                        }


                        // 然后再单个 metric 处理
                        for (Map.Entry<String, Metric> entry : metricMap.entrySet()) {
                            GroupMetricsModel model = new GroupMetricsModel();
                            model.setTimestamp(timestamp);
                            model.setGroupKey(entry.getKey());
                            model.getMetrics().put(entry.getKey(), new GroupMetricsModel.MetricModel(
                                    entry.getKey(),
                                    entry.getValue().getGauge().getDataPointsCount() > 0 ?
                                            entry.getValue().getGauge().getDataPoints(0).getAsInt() : 0
                            ));
                            out.collect(model);
                        }
                    }
                })
                .keyBy(new KeySelector<GroupMetricsModel, String>() {

                    // 定义了三组联合计算的指标 A+B、C+D、A+C
                    // 这个时候需要将 A 、B、C 、D 三个类型的指标路由到同一个的时间窗口聚合函数处理
                    // sw_jvm.cpu.usage and sw_jvm.thread.live.count
                    private final List<String> unionMetricList =
                            Arrays.asList("sw_jvm.cpu.usage", "sw_jvm.thread.live.count");

                    @Override
                    public String getKey(GroupMetricsModel value) throws Exception {
                        // 在这里控制联合的多个指标 key by 到同一个算子
                        return unionMetricList.contains(value.getGroupKey()) ? "union_key" : value.getGroupKey();
                    }
                })

                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<GroupMetricsModel, ImitateMetricData, ImitateMetricData>() {
                    @Override
                    public ImitateMetricData createAccumulator() {
                        return new ImitateMetricData();
                    }

                    @Override
                    public ImitateMetricData add(GroupMetricsModel value, ImitateMetricData accumulator) {

                        if (accumulator.getName() == null) {
                            accumulator.setName(value.getGroupKey());
                        }

                        for (GroupMetricsModel.MetricModel metric : value.getMetrics().values()) {
                            if (metric == null) {
                                continue;
                            }
                            accumulator.setValue(accumulator.getValue() + metric.getValue());
                        }


                        accumulator.setTimestamp(value.getTimestamp());
                        return accumulator;
                    }

                    @Override
                    public ImitateMetricData getResult(ImitateMetricData accumulator) {
                        return accumulator;
                    }

                    @Override
                    public ImitateMetricData merge(ImitateMetricData a, ImitateMetricData b) {
                        a.setValue(a.getValue() + b.getValue());
                        return a;
                    }
                })
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {
                        // 统一按照时间窗口对齐
                        value.setTimestamp(context.currentWatermark());
                        System.err.println(value);
                    }
                });

        environment.execute("kafka-otel-meters-example");
    }
}
