package ifrat.com.flink.examples;

import ifrat.com.flink.examples.otel.ExportMetricsServiceRequestUtil;
import ifrat.com.flink.examples.otel.OtelMetricAggregateFunction;
import ifrat.com.flink.examples.otel.UnionMetricAggResult;
import ifrat.com.flink.examples.otel.UnionMetricGroupByModel;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

/**
 * <pre>
 * broadcast:
 *  - connect-flat-map (2/4)#0 => SideEffectRule(op=>=, value=49.428779616798785)
 *  - connect-flat-map (1/4)#0 => SideEffectRule(op=>=, value=49.428779616798785)
 *  - connect-flat-map (4/4)#0 => SideEffectRule(op=>=, value=49.428779616798785)
 *  - connect-flat-map (3/4)#0 => SideEffectRule(op=>=, value=49.428779616798785)
 * </pre>
 *
 * <pre>
 * none broadcast:
 * - connect-flat-map (4/4)#0 => SideEffectRule(op=<=, value=23.660255630259286)
 * </pre>
 */
public class FlinkConnectBroadcastSourceExample018 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(10);

        DataStreamSource<ExportMetricsServiceRequest> dataStreamSource = environment.fromSource(ExportMetricsServiceRequestUtil.buildKafkaSource(),
                WatermarkStrategy.<ExportMetricsServiceRequest>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ExportMetricsServiceRequest>() {
                            @Override
                            public long extractTimestamp(ExportMetricsServiceRequest element, long recordTimestamp) {
                                return ExportMetricsServiceRequestUtil.extractTimestamp(element);
                            }
                        }), "otel-kafka-source");

        SingleOutputStreamOperator<SideEffectMultiMetricUnionRule> ruleSource = environment.addSource(new SideEffectDataSource()).name("rule");


        // 这里为什么需要 connect 一个 rule，那是因为需要根据指定的规则来设置 key by 的值
        // 对于一些多指标联合告警的场景，需要将多个指标 key 到同一个算子中处理
        KeyedStream<UnionMetricGroupByModel, String> keyedStream =
                dataStreamSource.connect(ruleSource.broadcast())
                        .flatMap(new RichCoFlatMapFunction<ExportMetricsServiceRequest, SideEffectMultiMetricUnionRule, UnionMetricGroupByModel>() {

                            private List<SideEffectMultiMetricUnionRule> rules = new LinkedList<>();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                            }

                            @Override
                            public void flatMap1(ExportMetricsServiceRequest value, Collector<UnionMetricGroupByModel> out) throws Exception {

                                List<UnionMetricGroupByModel> modelList = ExportMetricsServiceRequestUtil.extractMetric(value);

                                for (UnionMetricGroupByModel model : modelList) {
                                    // 这里根据下发过来的告警指标维度的规则，生产 group key
                                    model.setGroupKey(model.getName());
                                    out.collect(model);
                                }
                            }

                            @Override
                            public void flatMap2(SideEffectMultiMetricUnionRule value, Collector<UnionMetricGroupByModel> out) throws Exception {
                                // flatMap 可以没有输出，这里接收到上游 broadcast 下来的规则，作用与 flatMap1
                                rules.add(value);
                            }
                        })
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<UnionMetricGroupByModel>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UnionMetricGroupByModel>() {
                                    @Override
                                    public long extractTimestamp(UnionMetricGroupByModel element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                }))
                        .keyBy(new KeySelector<UnionMetricGroupByModel, String>() {
                            @Override
                            public String getKey(UnionMetricGroupByModel value) throws Exception {
                                return value.getGroupKey();
                            }
                        });

        // 这上游采集上来的指标周期是 1m

        // 设置指定的几个聚合时间窗口,1m、5m、10m、30m、60m
        // 1m 的时间窗口聚合
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new OtelMetricAggregateFunction())
                .addSink(new SinkFunction<UnionMetricAggResult>() {
                    @Override
                    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                });

        // 5m 的时间窗口聚合
        keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new OtelMetricAggregateFunction())
                .addSink(new SinkFunction<UnionMetricAggResult>() {
                    @Override
                    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                });

        // 10m 的时间窗口聚合
        keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new OtelMetricAggregateFunction())
                .addSink(new SinkFunction<UnionMetricAggResult>() {
                    @Override
                    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                });

        // 30m 的时间窗口聚合
        keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .aggregate(new OtelMetricAggregateFunction())
                .addSink(new SinkFunction<UnionMetricAggResult>() {
                    @Override
                    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                });

        // 1h 的
        keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new OtelMetricAggregateFunction())
                .addSink(new SinkFunction<UnionMetricAggResult>() {
                    @Override
                    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                });

        environment.execute("connect-example");
    }
}
