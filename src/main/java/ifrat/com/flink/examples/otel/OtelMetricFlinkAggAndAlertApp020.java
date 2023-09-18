package ifrat.com.flink.examples.otel;

import ifrat.com.flink.examples.SideEffectDataSource;
import ifrat.com.flink.examples.SideEffectMultiMetricUnionRule;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 事件定义
 * 简单事件(SEP Simple Event Process)：简单事件存在于现实场景中，主要的特点为处理单一事件，事件的定义可以直接观察出来，
 * 处理过程中无须关注多个事件之间的关系，能够通过简单的数据处理手段将结果计算出来。
 * 复杂事件(CEP Complex Event Process)：相对于简单事件，复杂事件处理的不仅是单一的事件，也处理由多个事件组成的复合事件。
 * 复杂事件处理监测分析事件流(Event Streaming)，当特定事件发生时来触发某些动作。
 * <p>
 * 用来处理指标的实时聚合与相关告警规则的计算，例如:
 * # Flink 简单事件处理:
 * 1. 某个指标的 min、max、sum、avg、
 * 2. 指定时间窗口出现的次数，阈值范围出现的次数
 * - 指定时间窗口出现的次数: 某一个服务每 5m 一个时间窗口的访问次数
 * - 阈值范围出现的次数: 某个接口每 5m 钟响应时间超过 3s 的一个次数
 * <p>
 * 3. 复杂时间的处理(实在简单事件处理之上的)，需要将多个事件的处理路由到同一个聚合算子上
 * - A 和 B 指标同时满足某个条件
 * - A 和 B 指标只要满足其中一个条件
 */
public class OtelMetricFlinkAggAndAlertApp020 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 这一行必须加，否则会出现 Collection#unmodifyied exception .
        environment.getConfig().registerTypeWithKryoSerializer(ExportMetricsServiceRequest.class,
                com.twitter.chill.protobuf.ProtobufSerializer.class);
        environment.getConfig().setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(3));

        //  还有其他的一些参数，正式上线时进行优化
        DataStreamSource<ExportMetricsServiceRequest> dataStreamSource =
                environment.fromSource(ExportMetricsServiceRequestUtil.buildKafkaSource(),
                                WatermarkStrategy.noWatermarks(), "otel-kafka-source")
                        // 当前测试阶段，这个并行度一定要和 topic 的分区数保持一致
                        .setParallelism(1);


        SingleOutputStreamOperator<SideEffectMultiMetricUnionRule> ruleDataSource = environment.
                addSource(new SideEffectDataSource()).name("rule-side-effect-source");

        KeyedStream<UnionMetricGroupByModel, String> keyedStream = dataStreamSource.connect(ruleDataSource.broadcast())
                .flatMap(new RichCoFlatMapFunction<ExportMetricsServiceRequest, SideEffectMultiMetricUnionRule, UnionMetricGroupByModel>() {

                    // 每一个指标他所涵盖的告警计算指标规则，需要带到下游的聚合算子，注意有可能这个指标没有配置聚合计算
                    private Map<String, Set<SideEffectMultiMetricUnionRule.MetricRuleItem>> metricClassRuleMap = new HashMap<>();
                    // 这里记录的是哪些个指标需要联合在一起，需要带到下游的聚合算子
                    private volatile int size = 0;
                    private List<SideEffectMultiMetricUnionRule.MetricAlertRule> unionMetricList = new LinkedList<>();
                    private final String unionGroupKey = UUID.randomUUID().toString().substring(0, 4);

                    @Override

                    public void flatMap1(ExportMetricsServiceRequest value, Collector<UnionMetricGroupByModel> out) throws Exception {

                        List<UnionMetricGroupByModel> unionMetricGroupByModelList = ExportMetricsServiceRequestUtil.extractMetric(value);
                        for (UnionMetricGroupByModel model : unionMetricGroupByModelList) {
                            String groupKey = model.getName();
                            String metricName = model.getName();

                            // 如果在联合计算里面，则优先使用
                            outer:
                            for (SideEffectMultiMetricUnionRule.MetricAlertRule unionMetric : unionMetricList) {
                                for (SideEffectMultiMetricUnionRule.MetricRuleItem ruleItem : unionMetric.getRuleItemSet()) {
                                    if (ruleItem.getName().equals(metricName)) {
                                        groupKey = unionGroupKey;
                                        model.setUnionGroupKey((byte) 1);
                                        // 2. 还需要解决当前哪些指标进行组合的问题,这个数据只需要带一次就可以，每次全量带下去
                                        if (this.size > 0) {
                                            model.setMetricAlertRuleList(unionMetricList);
                                            this.size = 0;
                                        }
                                        break outer;
                                    }
                                }
                            }
                            // 1. 解决联合指标路由到同一个聚合算子上的问题
                            model.setGroupKey(groupKey);

                            // 3. 这个也需要带下去，只需要带一遍即可，不需要每次都带下去
                            if (!metricClassRuleMap.isEmpty() && metricClassRuleMap.containsKey(metricName)) {
                                model.setMetricRuleSet(metricClassRuleMap.remove(metricName));
                            }
                            out.collect(model);
                        }
                    }

                    /**
                     * 第一个版本: 先简单处理，将所有需要联合告警的指标都路由到同一个聚合算子
                     * @param value The stream element
                     * @param out The collector to emit resulting elements to
                     * @throws Exception
                     */
                    @Override
                    public void flatMap2(SideEffectMultiMetricUnionRule value, Collector<UnionMetricGroupByModel> out) throws Exception {

                        // 1. 分解: 从所有的告警规则里面分解出每个指标需要进行哪些计算
                        Map<String, Set<SideEffectMultiMetricUnionRule.MetricRuleItem>> metricClassRuleMap = new HashMap<>();
                        // 2. 合并提取
                        List<SideEffectMultiMetricUnionRule.MetricAlertRule> unionMetricList = new LinkedList<>();

                        for (SideEffectMultiMetricUnionRule.MetricAlertRule alertRule : value.getAlertRuleMetricDemMap().values()) {

                            for (SideEffectMultiMetricUnionRule.MetricRuleItem metricRule : alertRule.getRuleItemSet()) {
                                // 将每个指标需要的告警聚合计算进行分解
                                Set<SideEffectMultiMetricUnionRule.MetricRuleItem> alertGroup =
                                        metricClassRuleMap.computeIfAbsent(metricRule.getName(), k -> new HashSet<>());

                                alertGroup.add(metricRule);
                            }

                            if (alertRule.getRuleItemSet().size() == 1) {
                                continue;
                            }


                            unionMetricList.add(alertRule);
                        }

                        this.metricClassRuleMap = metricClassRuleMap;
                        this.unionMetricList = unionMetricList;

                        this.size = metricClassRuleMap.size();

                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UnionMetricGroupByModel>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UnionMetricGroupByModel>() {
                            @Override
                            public long extractTimestamp(UnionMetricGroupByModel element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .name("otel-flatmap").setParallelism(8)
                .keyBy(new KeySelector<UnionMetricGroupByModel, String>() {
                    @Override
                    public String getKey(UnionMetricGroupByModel value) throws Exception {
                        return value.getGroupKey();
                    }
                });

        // 1m、5m、10m、15m、30、60m
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new OtelMetricAggregateFunction("1m"))
                .name("1m-agg-window").setParallelism(4)
                .addSink(new OtelExampleSinkFunction()).name("1m-agg-sink").setParallelism(1);

        keyedStream
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new OtelMetricAggregateFunction("5m"))
                .name("5m-agg-window").setParallelism(4)
                .addSink(new OtelExampleSinkFunction()).name("5m-agg-sink").setParallelism(1);


        environment.execute("otel-metric-agg");
    }
}
