package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ImitateMemoryMetricSource implements SourceFunction<ImitateMetricData> {
    @Override
    public void run(SourceContext ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
