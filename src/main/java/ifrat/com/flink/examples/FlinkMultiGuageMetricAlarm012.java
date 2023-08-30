package ifrat.com.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 如何将多个不同类型的指标 Group By 到一起，也就是 Group By 的规则是不固定的，可以是一个，也可以是多个。
 * Union 使用的一些场景:
 * 1. 多个 Source
 * 2. 每个 Source 中的元素 Schema 是一致的
 * 3. 不去重
 * 4. FIFO 的 dispatcher 模式
 * <p>
 * 和 Connect 的区别:
 * 1. 只能连接两个
 * 2. 数据类型可以不一致
 * 3. 使用场景是需要一些 SideEffect 应用在主流上的一些控制
 */
public class FlinkMultiGuageMetricAlarm012 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ImitateMetricData> rtMetricSource = environment.addSource(new ImitateMemoryMetricSource());

        DataStreamSource<ImitateMetricData> cpuLoadSource = environment.addSource(new ImitateCpuLoadMetricSource());

        // 将 rt、cpu 这两个指标进行 union 操作
        DataStream<ImitateMetricData> unionDataStream = rtMetricSource.union(cpuLoadSource);

        unionDataStream
                .keyBy(new KeySelector<ImitateMetricData, String>() {
                    @Override
                    public String getKey(ImitateMetricData value) throws Exception {
                        return null;
                    }
                })
                .map(new MapFunction<ImitateMetricData, ImitateMetricData>() {
                    @Override
                    public ImitateMetricData map(ImitateMetricData value) throws Exception {
                        return value;
                    }
                }).addSink(new SinkFunction<ImitateMetricData>() {
                    @Override
                    public void invoke(ImitateMetricData value, Context context) throws Exception {

                        System.err.println(value);
                    }
                });

        environment.execute("union example !");
    }
}
