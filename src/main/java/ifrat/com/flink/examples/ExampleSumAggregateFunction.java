package ifrat.com.flink.examples;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ExampleSumAggregateFunction implements AggregateFunction<ImitateMetricData, ImitateMetricData, ImitateMetricData> {

    private AtomicLong count = new AtomicLong();
    private String tag = "None";
    private Map<String, ImitateMetricData> ruleMap = new HashMap<>();

    public ExampleSumAggregateFunction() {
    }

    public ExampleSumAggregateFunction(String tag) {
        this.tag = tag;
    }

    @Override
    public ifrat.com.flink.examples.ImitateMetricData createAccumulator() {
        return new ifrat.com.flink.examples.ImitateMetricData();
    }

    @Override
    public ifrat.com.flink.examples.ImitateMetricData add(ImitateMetricData value, ImitateMetricData accumulator) {
        if ("rule".equals(value.getLable())) {
            // 在这里需要观察是否规则都到了所有的的聚合算子
            PrintStream printStream = count.incrementAndGet() % 2 == 0 ? System.err : System.out;
            printStream.println(Thread.currentThread().getName() + "=> " + tag + " => " + value + "; size=" + ruleMap.size());
            ruleMap.put(value.getName() + "_" + value.getOp(), value);
            return accumulator;
        }

        if (accumulator.getName() == null) {
            accumulator.setName(value.getName());
            accumulator.setOneLevelGroupKey(value.getOneLevelGroupKey());
        }

        accumulator.setValue(accumulator.getValue() + value.getValue());
        accumulator.setTimestamp(value.getTimestamp());


        return accumulator;
    }

    @Override
    public ifrat.com.flink.examples.ImitateMetricData getResult(ifrat.com.flink.examples.ImitateMetricData accumulator) {
        return accumulator;
    }

    @Override
    public ifrat.com.flink.examples.ImitateMetricData merge(ifrat.com.flink.examples.ImitateMetricData a, ifrat.com.flink.examples.ImitateMetricData b) {

        a.setValue(a.getValue() + b.getValue());

        return a;
    }
}
