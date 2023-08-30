package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class FlinkRtMetricFilterSumExample009 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
                new RestartStrategies.FailureRateRestartStrategyConfiguration(5,
                        Time.minutes(5), Time.seconds(10));

        environment.getConfig().setRestartStrategy(restartStrategyConfiguration);
        environment.getConfig().setAutoWatermarkInterval(3);

        environment.addSource(new ImitateRtMetricSource())
                .filter(new FilterFunction<ImitateMetricData>() {
                    @Override
                    public boolean filter(ImitateMetricData value) throws Exception {
                        return value.getValue() > 5.0D;
                    }
                }).name("value-filter")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ImitateMetricData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ImitateMetricData>() {
                            @Override
                            public long extractTimestamp(ImitateMetricData element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy("name")
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .sum("value")
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        System.err.println(value);
                    }
                }).name("sum-sink");

        environment.execute("filter sum example .");
    }
}
