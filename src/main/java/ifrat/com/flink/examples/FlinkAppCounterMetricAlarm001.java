package ifrat.com.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Date;

public class FlinkAppCounterMetricAlarm001 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.addSource(new ImitateRequestCountMetricSource())
                .map(new MapFunction<ImitateMetricData, String>() {
                    @Override
                    public String map(ImitateMetricData imitateMetricData) throws Exception {
                        return new Date(imitateMetricData.getTimestamp())
                                + ":" + imitateMetricData.getName()
                                + " ->" + imitateMetricData.getValue();
                    }
                }).addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        System.out.println(value);
                    }
                });

        environment.execute("counter metric alarm example");
    }
}
