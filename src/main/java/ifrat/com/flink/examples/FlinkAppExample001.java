package ifrat.com.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkAppExample001 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.addSource(new TimerEmmitSourceSource())
                .map(new MapFunction<EmmitSourceData, String>() {
                    @Override
                    public String map(EmmitSourceData emmitSourceData) throws Exception {
                        return emmitSourceData.getName()+" -> " + emmitSourceData.getValue();
                    }
                }).addSink(new SinkFunction<>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {

                        System.out.println(value);
                    }
                });

        environment.execute("flink first app example !");
    }
}
