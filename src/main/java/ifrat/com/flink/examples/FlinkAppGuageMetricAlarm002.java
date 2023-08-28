package ifrat.com.flink.examples;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public class FlinkAppGuageMetricAlarm002 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认使用的是 event time
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(1));
        environment.setParallelism(1);

        // 默认的就是 event time 的语义，因此不需要额外在进行设置
        environment.addSource(new ImitateRtMetricSource())
                .map(new MapFunction<ImitateMetricData, ImitateMetricData>() {
                    @Override
                    public ImitateMetricData map(ImitateMetricData imitateMetricData) throws Exception {
                        System.err.println(imitateMetricData.toString());
                        return imitateMetricData;
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<ImitateMetricData>() {

                            private long maxTimestamp;
                            private long delay = 0L;

                            @Nullable
                            @Override
                            public Watermark getCurrentWatermark() {

                                return new Watermark(maxTimestamp - delay);
                            }

                            @Override
                            public long extractTimestamp(ImitateMetricData element, long recordTimestamp) {

                                if (element.getTimestamp() > maxTimestamp) {
                                    maxTimestamp = element.getTimestamp();
                                }

                                return element.getTimestamp();
                            }
                        })
                .keyBy(new KeySelector<ImitateMetricData, String>() {

                    @Override
                    public String getKey(ImitateMetricData imitateMetricData) throws Exception {
                        return imitateMetricData.getName();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<ImitateMetricData, ImitateMetricData, ImitateMetricData>() {
                    @Override
                    public ImitateMetricData createAccumulator() {
                        return new ImitateMetricData();
                    }

                    @Override
                    public ImitateMetricData add(ImitateMetricData value, ImitateMetricData accumulator) {

                        accumulator.setName(value.getName());
                        if (accumulator.getValue() == null) {
                            accumulator.setName(value.getName());
                            accumulator.setTimestamp(value.getTimestamp());
                            accumulator.setValue(value.getValue());

                        } else if (accumulator.getValue() < value.getValue()) {
                            accumulator.setName(value.getName());
                            accumulator.setValue(value.getValue());
                            accumulator.setTimestamp(value.getTimestamp());
                        }

                        return accumulator;
                    }

                    @Override
                    public ImitateMetricData getResult(ImitateMetricData accumulator) {
                        return accumulator;
                    }

                    @Override
                    public ImitateMetricData merge(ImitateMetricData a, ImitateMetricData b) {
                        return a.getValue() > b.getValue() ? a : b;
                    }
                })
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        System.out.println(value.toString());
                    }
                });


        environment.execute("guage example");
    }
}
