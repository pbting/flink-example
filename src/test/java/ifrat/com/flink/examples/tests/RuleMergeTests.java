package ifrat.com.flink.examples.tests;

import ifrat.com.flink.examples.SideEffectMultiMetricUnionRule;
import org.junit.Test;

import java.util.*;

public class RuleMergeTests {

    /**
     * - A + B: count(A > threshold 1) And count( B > threshold )
     * - A + D: count(A > threshold 2) And count (D > threshold)
     * <p>
     * 要定义一个数据模型：
     * A: 有哪些
     */
    @Test
    public void testRuleMerge() {

    }

    @Test
    public void testDuplicate() {
        SideEffectMultiMetricUnionRule.MetricRuleItem cpuMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        cpuMetric.setName("A");
        cpuMetric.setCalType("max");
        cpuMetric.setOp(">");
        cpuMetric.setValue(80);

        final HashSet<SideEffectMultiMetricUnionRule.MetricRuleItem> metricRuleHashSet = new HashSet<>();
        metricRuleHashSet.add(cpuMetric);

        cpuMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        cpuMetric.setName("A");
        cpuMetric.setCalType("min");
        cpuMetric.setOp(">");
        cpuMetric.setValue(80);
        metricRuleHashSet.add(cpuMetric);


        System.err.println(metricRuleHashSet.size());
    }

    @Test
    public void testMerge() {

        // A + B
        SideEffectMultiMetricUnionRule rule = new SideEffectMultiMetricUnionRule();

        SideEffectMultiMetricUnionRule.MetricAlertRule alertRule = new SideEffectMultiMetricUnionRule.MetricAlertRule();

        SideEffectMultiMetricUnionRule.MetricRuleItem cpuMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        cpuMetric.setName("A");
        cpuMetric.setCalType("max");
        cpuMetric.setOp(">");
        cpuMetric.setValue(80);

        SideEffectMultiMetricUnionRule.MetricRuleItem apiRtMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        apiRtMetric.setName("B");
        apiRtMetric.setCalType("min");
        apiRtMetric.setOp(">=");
        apiRtMetric.setValue(3);
        alertRule.getRuleItemSet().add(cpuMetric);
        alertRule.getRuleItemSet().add(apiRtMetric);

        rule.getAlertRuleMetricDemMap().put(UUID.randomUUID().toString(), alertRule);

        // C
        SideEffectMultiMetricUnionRule.MetricRuleItem memoryMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        memoryMetric.setName("C");
        memoryMetric.setCalType("count");
        memoryMetric.setOp(">=");
        memoryMetric.setValue(6);

        SideEffectMultiMetricUnionRule.MetricAlertRule alertRule1 = new SideEffectMultiMetricUnionRule.MetricAlertRule();
        alertRule1.getRuleItemSet().add(memoryMetric);

        rule.getAlertRuleMetricDemMap().put(UUID.randomUUID().toString(), alertRule1);

        // B + D
        SideEffectMultiMetricUnionRule.MetricAlertRule alertRule2 = new SideEffectMultiMetricUnionRule.MetricAlertRule();

        SideEffectMultiMetricUnionRule.MetricRuleItem apiDRtMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        apiDRtMetric.setName("D");
        apiDRtMetric.setCalType("avg");
        apiDRtMetric.setOp(">=");
        apiDRtMetric.setValue(6);

        SideEffectMultiMetricUnionRule.MetricRuleItem apiBRtMetric = new SideEffectMultiMetricUnionRule.MetricRuleItem();
        apiBRtMetric.setName("B");
        apiBRtMetric.setCalType("min");
        apiBRtMetric.setOp(">=");
        apiBRtMetric.setValue(7);

        alertRule2.getRuleItemSet().add(apiDRtMetric);
        alertRule2.getRuleItemSet().add(apiBRtMetric);

        rule.getAlertRuleMetricDemMap().put(UUID.randomUUID().toString(), alertRule2);


        // 1. 分解: 从所有的告警规则里面分解出每个指标需要进行哪些计算
        Map<String, Set<SideEffectMultiMetricUnionRule.MetricRuleItem>> metricClassRuleMap = new HashMap<>();
        // 2. 合并提取
        List<SideEffectMultiMetricUnionRule.MetricAlertRule> unionMetricList = new LinkedList<>();

        for (SideEffectMultiMetricUnionRule.MetricAlertRule metricAlertRule : rule.getAlertRuleMetricDemMap().values()) {

            for (SideEffectMultiMetricUnionRule.MetricRuleItem metricRule : metricAlertRule.getRuleItemSet()) {
                // 将每个指标需要的告警聚合计算进行分解
                Set<SideEffectMultiMetricUnionRule.MetricRuleItem> alertGroup =
                        metricClassRuleMap.computeIfAbsent(metricRule.getName(), k -> new HashSet<>());

                alertGroup.add(metricRule);
            }

            if (metricAlertRule.getRuleItemSet().size() == 1) {
                continue;
            }


            unionMetricList.add(metricAlertRule);
        }

        System.err.println(metricClassRuleMap);

        System.out.println(unionMetricList);
    }

}
