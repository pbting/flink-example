package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class FlinkRtMetricFilterReduceExample011 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.addSource(new ImitateRtMetricSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ImitateMetricData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ImitateMetricData>() {
                            @Override
                            public long extractTimestamp(ImitateMetricData element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy("name")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ImitateMetricData>() {
                    @Override
                    public ImitateMetricData reduce(ImitateMetricData value1, ImitateMetricData value2) throws Exception {

                        System.err.println(Thread.currentThread().getName() + "; value1:" + value1 + "; value2:" + value2);
                        ImitateMetricData result = new ImitateMetricData();
                        result.setName(value1.getName());
                        result.setValue(value1.getValue() + value2.getValue());
                        result.setTimestamp(System.currentTimeMillis());

                        return result;
                    }
                }).setParallelism(4).name("reduce-sum")
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        System.out.println(value);
                    }
                });

        environment.execute("reduce-example");
    }
}
