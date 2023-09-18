package ifrat.com.flink.examples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SideEffectDataSource implements SourceFunction<SideEffectMultiMetricUnionRule> {
    private boolean isCancel = false;

    @Override
    public void run(SourceContext<SideEffectMultiMetricUnionRule> ctx) throws Exception {

        final String[] ops = new String[]{">", ">=", "<=", "<",};
        // sw_jvm.cpu.usage and sw_jvm.thread.live.count 这两个指标
        while (!isCancel) {
            SideEffectMultiMetricUnionRule rule = new SideEffectMultiMetricUnionRule();

            // 1. 准备一条联合告警
            SideEffectMultiMetricUnionRule.MetricAlertRule metricAlertRule = new SideEffectMultiMetricUnionRule.MetricAlertRule();
            SideEffectMultiMetricUnionRule.MetricRuleItem cpuMetricItem = new SideEffectMultiMetricUnionRule.MetricRuleItem();
            cpuMetricItem.setName("sw_jvm.cpu.usage");
            cpuMetricItem.setOp(">");
            cpuMetricItem.setValue(80);

            SideEffectMultiMetricUnionRule.MetricRuleItem apiRtMetricItem = new SideEffectMultiMetricUnionRule.MetricRuleItem();
            apiRtMetricItem.setName("sw_jvm.thread.live.count");
            apiRtMetricItem.setOp(">=");
            apiRtMetricItem.setValue(3);

            metricAlertRule.getRuleItemSet().add(cpuMetricItem);
            metricAlertRule.getRuleItemSet().add(apiRtMetricItem);

            rule.getAlertRuleMetricDemMap().put(UUID.randomUUID().toString(), metricAlertRule);

            // 2. 准备一条单独告警的
            apiRtMetricItem = new SideEffectMultiMetricUnionRule.MetricRuleItem();
            apiRtMetricItem.setName("sw_jvm.thread.live.count");
            apiRtMetricItem.setOp(">=");
            apiRtMetricItem.setValue(30);

            metricAlertRule = new SideEffectMultiMetricUnionRule.MetricAlertRule();
            metricAlertRule.getRuleItemSet().add(apiRtMetricItem);

            rule.getAlertRuleMetricDemMap().put(UUID.randomUUID().toString(), metricAlertRule);

            ctx.collect(rule);

            TimeUnit.HOURS.sleep(1);
        }
    }

    @Override
    public void cancel() {

        isCancel = true;
    }
}
