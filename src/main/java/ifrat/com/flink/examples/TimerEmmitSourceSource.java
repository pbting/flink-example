package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class TimerEmmitSourceSource implements SourceFunction<EmmitSourceData> {

    private boolean isCancel = false;

    @Override
    public void run(SourceContext<EmmitSourceData> ctx) throws Exception {
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

            ctx.collect(sourceData);

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
