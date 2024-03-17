package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 模拟1个 Source，两个时间窗口聚合计算的算子，数据倾斜的问题
 */
public class FlinkEventTimeSkewApp01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 watermark 周期性生成时间
        environment.getConfig().setAutoWatermarkInterval(Duration.ofSeconds(1).toMillis());
        // 模拟生成数据的 source 算子，要求 key by 的值固定为 1 个，表示产生数据倾斜

        // source Function 分为单并行度的和多并行度的，ParallelSourceFunction 和 RichSourceFunction
        RichParallelSourceFunction<EmmitSourceData> sourceFunction = new RichParallelSourceFunction<EmmitSourceData>() {
            boolean isCancel = false;
            private long lastTimestamp;
            private long sleep = 1;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 这段代码模拟 source 中其中一个算子长时间没有数据，观察 watermark 的推进情况
                int indexOfSubtask = this.getRuntimeContext().getIndexOfThisSubtask();
                System.err.println("index of subtask: " + indexOfSubtask);
                sleep = indexOfSubtask % 2 == 0 ? 1 : TimeUnit.MINUTES.toMillis(1);
            }

            @Override
            public void run(SourceContext<EmmitSourceData> ctx) throws Exception {

                while (!isCancel) {
                    EmmitSourceData emmitSourceData = new EmmitSourceData("man", new Random().nextInt(100));

                    // 这段代码模拟数据乱序的情况，晚点的数据提前到来
//                    if (lastTimestamp == 0 || TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - lastTimestamp) >= 2) {
//                        emmitSourceData.setEventTimestamp(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2));
//                        lastTimestamp = System.currentTimeMillis();
//                    } else {
//                        emmitSourceData.setEventTimestamp(System.currentTimeMillis());
//                    }

                    emmitSourceData.setEventTimestamp(System.currentTimeMillis());
                    ctx.collect(emmitSourceData);

                    try {
                        TimeUnit.MILLISECONDS.sleep(sleep);
                    } catch (InterruptedException e) {
                        // nothing to do
                    }
                }
            }

            @Override
            public void cancel() {
                isCancel = true;
            }
        };

        // 2 个 source 并行度，模拟其中一个算子长时间没有数据
        DataStream<EmmitSourceData> streamSource =
                environment.addSource(sourceFunction).name("mock-source").setParallelism(2)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<EmmitSourceData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<EmmitSourceData>() {
                                    @Override
                                    public long extractTimestamp(EmmitSourceData element, long recordTimestamp) {
                                        return element.getEventTimestamp();
                                    }
                                    //withIdleness 解决某个 Source 算子长时间没有数据的问题，强制对齐
                                }).withIdleness(Duration.ofSeconds(10)));

        DataStreamSink<EmmitSourceAccData> aggStream = streamSource.keyBy(new KeySelector<EmmitSourceData, String>() {
                    @Override
                    public String getKey(EmmitSourceData value) throws Exception {
                        return value.getName();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<EmmitSourceData, EmmitSourceAccData, EmmitSourceAccData>() {
                    @Override
                    public EmmitSourceAccData createAccumulator() {
                        EmmitSourceAccData accData = new EmmitSourceAccData();
                        accData.setOp("count");
                        return accData;
                    }

                    @Override
                    public EmmitSourceAccData add(EmmitSourceData value, EmmitSourceAccData accumulator) {

                        if (accumulator.getName() == null) {
                            accumulator.setName(value.getName());
                        }

                        accumulator.setVal(value.getValue() + accumulator.getVal());

                        return accumulator;
                    }

                    @Override
                    public EmmitSourceAccData getResult(EmmitSourceAccData accumulator) {
                        return accumulator;
                    }

                    @Override
                    public EmmitSourceAccData merge(EmmitSourceAccData a, EmmitSourceAccData b) {
                        return null;
                    }
                }).name("aggregate").setParallelism(2)
                .addSink(new SinkFunction<EmmitSourceAccData>() {
                    @Override
                    public void invoke(EmmitSourceAccData value, Context context) throws Exception {

                        value.setWindowTimestamp(new Date(context.currentWatermark()).toString());

                        System.err.println(value);
                    }

                    @Override
                    public void writeWatermark(Watermark watermark) throws Exception {
                        SinkFunction.super.writeWatermark(watermark);
                    }

                    @Override
                    public void finish() throws Exception {
                        SinkFunction.super.finish();
                    }
                }).name("sink").setParallelism(1);

        System.out.printf(environment.getExecutionPlan());
        environment.execute("event time skew");
    }
}
