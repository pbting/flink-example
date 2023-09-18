package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class SideEffectWatermarkSource implements SourceFunction<ImitateMetricData> {
    private boolean isCancel = false;

    public static final String VIRTUAL = "virtual";

    @Override
    public void run(SourceContext<ImitateMetricData> ctx) throws Exception {

        final ThreadLocalRandom localRandom = ThreadLocalRandom.current();

        while (!isCancel) {

            ImitateMetricData imitateMetricData = new ImitateMetricData();
            imitateMetricData.setName(VIRTUAL);
            imitateMetricData.setValue(localRandom.nextDouble(100));
            imitateMetricData.setTimestamp(System.currentTimeMillis());

            ctx.collect(imitateMetricData);

            TimeUnit.SECONDS.sleep(1);
        }

    }

    @Override
    public void cancel() {

        isCancel = true;
    }
}
