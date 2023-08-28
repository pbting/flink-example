package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class FlinkAppWaterMarkStrategy006 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        environment.addSource(new ImitateRtMetricSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 注意这里的泛型一定要指定
                                .<ImitateMetricData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .keyBy("name")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .max("value")
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        System.err.println(value.toString());
                    }
                });

        environment.execute("water mark strategy example001");
    }
}
