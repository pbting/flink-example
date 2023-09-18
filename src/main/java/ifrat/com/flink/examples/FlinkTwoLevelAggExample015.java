package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FlinkTwoLevelAggExample015 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(3));

        environment.addSource(new ImitateRtMetricSource())
                .setParallelism(2)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ImitateMetricData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ImitateMetricData>() {
                            @Override
                            public long extractTimestamp(ImitateMetricData element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(new KeySelector<ImitateMetricData, String>() {
                    @Override
                    public String getKey(ImitateMetricData value) throws Exception {
//                        String groupKey = value.getName() + "_" + ThreadLocalRandom.current().nextInt(4) + 1;
                        return value.getOneLevelGroupKey();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new ExampleSumAggregateFunction()).name("one-level-agg").setParallelism(4)
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        if (value.getValue() > 20) {
                            System.out.println(value);
                        }
                    }
                });

        environment.execute("second-level-agg-example");

    }
}
