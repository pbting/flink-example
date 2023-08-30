package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class FlinkRtMetricFilterExample008 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(10L);

        environment.addSource(new ImitateRtMetricSource())
                .filter(new FilterFunction<ImitateMetricData>() {
                    @Override
                    public boolean filter(ImitateMetricData value) throws Exception {
                        return value.getValue() > 5.0D;
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ImitateMetricData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ImitateMetricData>() {
                                    @Override
                                    public long extractTimestamp(ImitateMetricData element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                }))
                .keyBy(new KeySelector<ImitateMetricData, String>() {
                    @Override
                    public String getKey(ImitateMetricData value) throws Exception {
                        return value.getName();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .max("value")
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        System.err.println(value);
                    }
                });

        environment.execute("filter window example");
    }
}
