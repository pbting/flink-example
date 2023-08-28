package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于模拟 Counter 类型的指标数据源如何计算变化率进行监控告警。
 * 例如: 正常来讲一个服务总的请求数应该是持续递增的，如果监控到某个时间段一直不增长，说明没有请求量了，需要进行监控告警。
 */
public class ImitateRequestCountMetricSource implements SourceFunction<ImitateMetricData> {

    private boolean flag = false;

    @Override
    public void run(SourceContext<ImitateMetricData> ctx) throws Exception {

        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger restTimes = new AtomicInteger();
        ImitateMetricData lastEmmit = null;
        while (!flag) {

            //
            if (count.get() == 0 || count.get() % 20 != 0) {
                ImitateMetricData metricData = new ImitateMetricData();
                metricData.setName("request.count");

                // 每到 100 次，持续 3 个 5s 不增，以此类推，下游算子应该可以监控到滑动窗口为 30s ，滑动的步长为 5s 进行监控。
                metricData.setValue(count.incrementAndGet() * 5.0);
                ctx.collect(metricData);
                lastEmmit = metricData;
            } else if (restTimes.incrementAndGet() > 15) {
                // 过了 15s 的 休息时间，重置到 0
                count.incrementAndGet();
                restTimes.set(0);
            } else {
                ctx.collect(lastEmmit);
            }

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        flag = true;
    }
}
