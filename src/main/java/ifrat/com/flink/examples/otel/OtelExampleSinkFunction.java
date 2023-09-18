package ifrat.com.flink.examples.otel;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

public class OtelExampleSinkFunction implements SinkFunction<UnionMetricAggResult> {

    @Override
    public void invoke(UnionMetricAggResult value, Context context) throws Exception {
        // 1. 先处理单指标的
        if (value.getUnionMetricAggResultMap().isEmpty()) {
            System.err.println("单个指标: " + value);
        } else {
            // 2. 处理多指标的
            for (UnionMetricAggResult agg : value.getUnionMetricAggResultMap().values()) {
                System.out.println("联合指标单个输出: " + agg);
            }

            // 3. 联合指标输出
            for (List<UnionMetricAggResult> list : value.getUnionOtherMetric()) {
                System.err.println("联合指标输出: " + list);
            }
        }
    }
}
