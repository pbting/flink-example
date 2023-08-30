package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 注意: SourceFunction 和 ParallelSourceFunction 的区别
 */
public class ImitateRtMetricSource implements ParallelSourceFunction<ImitateMetricData> {
    boolean isCancel = false;
    @Override
    public void run(SourceContext<ImitateMetricData> ctx) throws Exception {

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final String[] rtArray = new String[]{
                "api_001_rt", "api_002_rt", "api_003_rt", "api_004_rt"
        };
        final int[] arrValue = new int[]{1, 2, 3, 4};

        while (!isCancel) {

            ImitateMetricData metricData = new ImitateMetricData();
            metricData.setOneLevelGroupKey("one_level_" + arrValue[ThreadLocalRandom.current().nextInt(arrValue.length)]);
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
