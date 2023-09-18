package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

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
public class FlinkConnectSourceExample017 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(10);

        SingleOutputStreamOperator<ImitateMetricData> rtDataSource =
                environment.addSource(new ImitateRtMetricSource()).name("rt-metric-source").setParallelism(2);

        SingleOutputStreamOperator<ImitateMetricData> cpuDataSource =
                environment.addSource(new ImitateCpuLoadMetricSource()).name("cpu-metric-source");

        DataStream<ImitateMetricData> dataStream = rtDataSource.union(cpuDataSource);

        DataStream<SideEffectMultiMetricUnionRule> ruleSource = environment.addSource(new SideEffectDataSource()).name("side-effect-source");

        // 第一次 connect, 获取哪些指标是需要联合计算的
        KeyedStream<ImitateMetricData, String> keyedDataStream = dataStream.
                connect(ruleSource.broadcast())
                .flatMap(new CoFlatMapFunction<ImitateMetricData, SideEffectMultiMetricUnionRule, ImitateMetricData>() {
                    @Override
                    public void flatMap1(ImitateMetricData value, Collector<ImitateMetricData> out) throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void flatMap2(SideEffectMultiMetricUnionRule value, Collector<ImitateMetricData> out) throws Exception {
                        // 这里接收到 union rule，是用来控制 flat map1
                    }
                }).name("connect-flat-map").setParallelism(4)
                .keyBy(new KeySelector<ImitateMetricData, String>() {

                    @Override
                    public String getKey(ImitateMetricData value) throws Exception {
                        return value.getOneLevelGroupKey();
                    }
                });

        // 第二次 connect ,是为了解决计算指定阈值范围的次数。这里不需要 broadcast,
        // 在 flatmap 算子里面通过 key by 的处理来控制当前这个规则需要路由到所有的计算窗口
        DataStream<SideEffectMultiMetricUnionRule> calRule =
                environment.addSource(new SideEffectDataSource()).name("side-effect-cal-rule-source");

        KeyedStream<ImitateMetricData, String> calRuleKeyedStream = keyedDataStream
                .connect(calRule)
                .flatMap(new RichCoFlatMapFunction<ImitateMetricData, SideEffectMultiMetricUnionRule, ImitateMetricData>() {
                    @Override
                    public void flatMap1(ImitateMetricData value, Collector<ImitateMetricData> out) throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void flatMap2(SideEffectMultiMetricUnionRule value, Collector<ImitateMetricData> out) throws Exception {

                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ImitateMetricData>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ImitateMetricData>() {
                            @Override
                            public long extractTimestamp(ImitateMetricData element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(new KeySelector<ImitateMetricData, String>() {
                    @Override
                    public String getKey(ImitateMetricData value) throws Exception {
                        return value.getOneLevelGroupKey();
                    }
                });


        calRuleKeyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new ExampleSumAggregateFunction("5s"))
                .name("5s-window-agg").setParallelism(8)
                .addSink(new SinkFunction<ImitateMetricData>() {

                    private final AtomicLong incre = new AtomicLong();

                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {
                        if (value.getName() == null) {
                            return;
                        }

                        if (incre.incrementAndGet() % 2 == 0) {
                            System.err.println(Thread.currentThread().getName() + " => 5s =>  " + value.toString());
                        } else {
                            System.out.println(Thread.currentThread().getName() + " => 5s =>  " + value.toString());
                        }
                    }
                    // 到时候这个并行度可以通过参数进行传递
                }).name("5s-window-agg-sink").setParallelism(1);


        calRuleKeyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new ExampleSumAggregateFunction("10s"))
                .name("10s-window-agg").setParallelism(8)
                .addSink(new SinkFunction<ImitateMetricData>() {

                    private final AtomicLong incre = new AtomicLong();

                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        if (value.getName() == null) {
                            return;
                        }

                        if (incre.incrementAndGet() % 2 == 0) {
                            System.err.println(Thread.currentThread().getName() + " => 10s =>  " + value.toString());
                        } else {
                            System.out.println(Thread.currentThread().getName() + " => 10s =>  " + value.toString());
                        }
                    }
                }).name("10s-window-agg").setParallelism(1);

        environment.execute("connect-example");
    }
}
