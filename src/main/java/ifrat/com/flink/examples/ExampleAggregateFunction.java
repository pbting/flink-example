package ifrat.com.flink.examples;

import org.apache.flink.api.common.functions.AggregateFunction;

public class ExampleAggregateFunction implements AggregateFunction<ImitateMetricData, ImitateMetricData, ImitateMetricData> {
    @Override
    public ifrat.com.flink.examples.ImitateMetricData createAccumulator() {
        return new ifrat.com.flink.examples.ImitateMetricData();
    }

    @Override
    public ifrat.com.flink.examples.ImitateMetricData add(ImitateMetricData value, ImitateMetricData accumulator) {
        if (accumulator.getName() == null) {
            accumulator.setName(value.getName());
            accumulator.setOneLevelGroupKey(value.getOneLevelGroupKey());
        }

        accumulator.setValue(accumulator.getValue() + value.getValue());
        accumulator.setTimestamp(value.getTimestamp());

        System.out.print(value.getOneLevelGroupKey() + " => ");
        System.err.println(Thread.currentThread().getName() + "-> " + value.getName());
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
