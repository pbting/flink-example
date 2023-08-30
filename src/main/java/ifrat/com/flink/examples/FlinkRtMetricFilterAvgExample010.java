package ifrat.com.flink.examples;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;

public class FlinkRtMetricFilterAvgExample010 {

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
                .filter(new FilterFunction<ImitateMetricData>() {
                    @Override
                    public boolean filter(ImitateMetricData value) throws Exception {
                        return value.getValue() > 5.0D;
                    }
                })

                .keyBy("name")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<ImitateMetricData, RtAvgAccModel, RtAvgAccModel>() {
                    @Override
                    public RtAvgAccModel createAccumulator() {
                        return new RtAvgAccModel();
                    }

                    @Override
                    public RtAvgAccModel add(ImitateMetricData value, RtAvgAccModel accumulator) {

                        if (accumulator.getName() == null) {
                            // process first
                            accumulator.setName(value.getName());
                            accumulator.setTimestamp(value.getTimestamp());
                            accumulator.getList().add(value.getValue());
                        } else {
                            accumulator.setTimestamp(value.getTimestamp());
                            accumulator.getList().add(value.getValue());
                        }

                        return accumulator;
                    }

                    @Override
                    public RtAvgAccModel getResult(RtAvgAccModel accumulator) {

                        // 计算平均值
                        double sum = 0.0D;
                        for (double val : accumulator.getList()) {
                            sum += val;
                        }
                        accumulator.setAvg(sum / accumulator.getList().size());
                        return accumulator;
                    }

                    @Override
                    public RtAvgAccModel merge(RtAvgAccModel a, RtAvgAccModel b) {
                        // 少的合并到多的那一个
                        List<Double> list = a.getList();
                        if (!list.isEmpty()) {
                            b.getList().addAll(list);
                        }

                        return b;

                    }
                }).
                name("sum-avg-operator")
                .setParallelism(4)
                .addSink(new SinkFunction<RtAvgAccModel>() {
                    @Override
                    public void invoke(RtAvgAccModel value, Context context) throws Exception {

                        System.err.println(value);
                    }
                }).name("sink").setParallelism(1);

        environment.execute("sum-avg-example");
    }
}
