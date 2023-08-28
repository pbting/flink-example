package ifrat.com.flink.examples;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class FlinkAppGuageMetricPunctuateWatermark004 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(1));
        environment.setParallelism(1);

        environment.addSource(new ImitateRtMetricSource())
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ImitateMetricData>() {

                    private long maxTimestamp = 0;

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(ImitateMetricData lastElement, long extractedTimestamp) {

                        System.err.println(new Date() + " => " + "check and get next waterwarm: " + lastElement.getTimestamp());

                        // 如果当前的这个元素时间比观察到的时间搓要大，则

                        return lastElement.getTimestamp() > maxTimestamp ? new Watermark(maxTimestamp) : null;
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
                    public String getKey(ImitateMetricData value) throws Exception {
                        return value.getName();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .max("value")
                .addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {
                        System.out.println(value.toString());
                    }
                });

        environment.execute("punctuate watermark example");
    }

}
