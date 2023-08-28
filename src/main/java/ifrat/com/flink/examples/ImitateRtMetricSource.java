package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ImitateRtMetricSource implements SourceFunction<ImitateMetricData> {
    boolean isCancel = false;
    @Override
    public void run(SourceContext<ImitateMetricData> ctx) throws Exception {

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final String[] rtArray = new String[]{
                "api_001_rt", "api_002_rt", "api_003_rt", "api_004_rt"
        };

        while (!isCancel) {

            ImitateMetricData metricData = new ImitateMetricData();
            metricData.setName(rtArray[random.nextInt(rtArray.length)]);
            metricData.setValue(random.nextDouble(10));
            metricData.setTimestamp(System.currentTimeMillis());

            ctx.collect(metricData);

            TimeUnit.MILLISECONDS.sleep(random.nextInt(1000));
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
