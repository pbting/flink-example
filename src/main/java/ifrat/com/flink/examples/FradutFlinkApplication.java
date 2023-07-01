package ifrat.com.flink.examples;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 第一版本实现:
 * 1. 直接实现前后两次额交易额是否相差太大。真实的场景案例中有可能是前后两次交易的例子符合规则的前提下时间可能过了 2 天甚至更长，那这种情况该怎么来处理呢，是判断合理的交易呢还是
 *    有欺诈嫌疑的交易呢
 *
 * 2. 这个时候可以通过引入 Flink 的一些机制使得这个状态在指定的 TTL 时间范围内有效，超出后将清除，一个周期介绍开始下一个周期的检测
 */
public class FradutFlinkApplication {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.addSource(new SourceFunction<EmmitSourceData>() {
            private boolean isCancel = false;

            @Override
            public void run(SourceContext<EmmitSourceData> sourceContext) throws Exception {

                final String[] name = new String[]{
                        "zhangsan",
                        "lisi",
                        "wangwu",
                        "lide"
                };

                final ThreadLocalRandom localRandom = ThreadLocalRandom.current();

                while (!isCancel) {
                    String nameVal = name[localRandom.nextInt(name.length)];
                    double doubleValue = localRandom.nextDouble() * 100;
                    EmmitSourceData sourceData = new EmmitSourceData(nameVal,doubleValue);

                    sourceContext.collect(sourceData);

                    TimeUnit.MILLISECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                isCancel = true;
            }
        }).keyBy(new KeySelector<EmmitSourceData, String>() {
            @Override
            public String getKey(EmmitSourceData emmitSourceData) throws Exception {
                return emmitSourceData.getName();
            }
        }).process(new KeyedProcessFunction<String, EmmitSourceData, DetectResult>() {

            private transient ValueState<Double> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 对于高可靠的一些场景，需要基于 Flink 提供状态容错的 ValueState 原语来保证程序在极端宕机异常退出的场景下保证可用性。
                ValueStateDescriptor<Double> valueStateDescriptor = new ValueStateDescriptor<>("flag", Double.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(EmmitSourceData emmitSourceData,
                                       KeyedProcessFunction<String, EmmitSourceData, DetectResult>.Context context,
                                       Collector<DetectResult> collector) throws Exception {

                Double isLower = valueState.value();

                if (isLower != null) {
                    if (emmitSourceData.getValue() > 50) {
                        DetectResult detectResult = new DetectResult(emmitSourceData.getName(), isLower, emmitSourceData.getValue());
                        collector.collect(detectResult);
                    }

                    valueState.clear();
                }

                if (emmitSourceData.getValue() < 10) {
                    valueState.update(emmitSourceData.getValue());
                }
            }
        }).addSink(new SinkFunction<DetectResult>() {
            @Override
            public void invoke(DetectResult value, Context context) throws Exception {

                System.err.println("detect result: "+ value.toString());
            }
        });

        System.out.printf(environment.getExecutionPlan());

        environment.execute("exception detect");
    }
}
