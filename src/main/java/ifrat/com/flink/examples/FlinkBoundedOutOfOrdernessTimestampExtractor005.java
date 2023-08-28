package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkBoundedOutOfOrdernessTimestampExtractor005 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        environment.addSource(new ImitateRtMetricSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ImitateMetricData>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(ImitateMetricData element) {
                        return element.getTimestamp();
                    }
                })
                .keyBy("name")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .max("value")
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {
                        System.err.println(value.toString());
                    }
                });

        environment.execute("BoundedOutOfOrdernessTimestampExtractor");
    }
}
