package ifrat.com.flink.examples.otel;

import ifrat.com.flink.examples.SideEffectMultiMetricUnionRule;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;

/**
 * 一期: 预设几个内置的聚合计算的算子:
 * - 指标本身应该具有的计算: sum、min、max、avg、
 * - 指定阈值范围出现的次数，例如 内存 超过 xxx。
 * - 指标在当前窗口出现的总次数。附加额外的。
 */
public class OtelMetricAggregateFunction implements AggregateFunction<UnionMetricGroupByModel, UnionMetricAggResult, UnionMetricAggResult> {

    private Map<String, Set<SideEffectMultiMetricUnionRule.MetricRuleItem>> metricClassRuleMap = new HashMap<>();
    private List<SideEffectMultiMetricUnionRule.MetricAlertRule> unionMetricList = new LinkedList<>();

    private String tag = "None";

    public OtelMetricAggregateFunction() {
    }

    public OtelMetricAggregateFunction(String tag) {
        this.tag = tag;
    }

    @Override
    public UnionMetricAggResult createAccumulator() {
        return new UnionMetricAggResult();
    }

    /**
     * 注意: 这里面有可能多个联合指标备份到同一个组
     * 联合指标的聚合计算最复杂有可能这种情况(同一个指标 A 在不同的组合下计算方式不一样):
     * A + B:
     * - count(A > V1)
     * - max(B) > V2
     * A + C：
     * - count(A > V3)
     * - avg(C) > v4
     *
     * @param value       The value to add
     * @param accumulator The accumulator to add the value to
     * @return
     */
    @Override
    public UnionMetricAggResult add(UnionMetricGroupByModel value, UnionMetricAggResult accumulator) {

        if (value.getMetricAlertRuleList() != null && !value.getMetricAlertRuleList().isEmpty()) {
            System.err.println(Thread.currentThread().getName() + "=> (" + tag + ")receive union metric: " + value.getMetricAlertRuleList());
            unionMetricList.addAll(value.getMetricAlertRuleList());
        }

        if (value.getMetricRuleSet() != null && !value.getMetricRuleSet().isEmpty()) {
            // 更新，某个指标对应的告警聚合计算的集合
            System.out.println(Thread.currentThread().getName() + " => " + value.getName() + "=> (" + tag + ")指标告警聚合计算 => " + value.getMetricRuleSet());
            metricClassRuleMap.put(value.getName(), value.getMetricRuleSet());
        }

        if (value.getUnionGroupKey() == (byte) 1) {
            // 多指标联合处理
            processUnionMetricAgg(value, accumulator);
        } else {
            processSingleMetricAgg(value, accumulator);
        }

        return accumulator;
    }

    /**
     * 如何做联合指标的组合 进行输出
     *
     * @param accumulator The accumulator of the aggregation
     * @return
     */
    @Override
    public UnionMetricAggResult getResult(UnionMetricAggResult accumulator) {

        Map<String, UnionMetricAggResult> unionMetricAggResultMap = accumulator.getUnionMetricAggResultMap();

        if (unionMetricAggResultMap.isEmpty()) {
            // 没有联合指标
            accumulator.setAvg((double) accumulator.getSum() / accumulator.getCount());
            return accumulator;
        }

        // 处理联合指标的计算输出,这里是有哪些个指标需要联合在一起进行输出
        final Set<List<UnionMetricAggResult>> unionMetricAggResult = new LinkedHashSet<>();
        for (SideEffectMultiMetricUnionRule.MetricAlertRule unionMetric : unionMetricList) {

            List<UnionMetricAggResult> unionAggResultList = new LinkedList<>();
            for (SideEffectMultiMetricUnionRule.MetricRuleItem rule : unionMetric.getRuleItemSet()) {
                // 依次遍历，取出联合的指标
                unionAggResultList.add(unionMetricAggResultMap.get(rule.getName()));
            }
            unionMetricAggResult.add(unionAggResultList);
        }

        accumulator.setUnionOtherMetric(unionMetricAggResult);

        return accumulator;
    }

    @Override
    public UnionMetricAggResult merge(UnionMetricAggResult a, UnionMetricAggResult b) {
        return null;
    }

    /**
     * 多指标的聚合计算，存在 acc 里面的 map
     *
     * @param value
     * @param accumulator
     */
    private void processUnionMetricAgg(UnionMetricGroupByModel value, UnionMetricAggResult accumulator) {

        UnionMetricAggResult aggResult = accumulator.getUnionMetricAggResultMap().get(value.getName());

        if (aggResult == null) {
            aggResult = new UnionMetricAggResult();
            accumulator.getUnionMetricAggResultMap().put(value.getName(), aggResult);
        }

        processSingleMetricAgg(value, aggResult);
    }

    /**
     * 单个指标的聚合计算，计算完之后的结果直接存在 accumulator
     *
     * @param value
     * @param accumulator
     */
    private void processSingleMetricAgg(UnionMetricGroupByModel value, UnionMetricAggResult accumulator) {
        if (accumulator.getGroupKey() == null) {
            accumulator.setGroupKey(value.getGroupKey());
            accumulator.setName(value.getName());
            accumulator.setTimestamp(value.getTimestamp());
        }

        // 1. 计算 min
        long currentVal = value.getValue();
        if (accumulator.getMinVal() <= 0 || currentVal < accumulator.getMinVal()) {
            accumulator.setMinVal(currentVal);
        }

        // 2. 计算 max
        if (currentVal > accumulator.getMaxVal()) {
            accumulator.setMaxVal(currentVal);
        }

        // 3. 计算 sum, 同时统计 count，以便后面计算 avg
        accumulator.setCount(accumulator.getCount() + 1);
        accumulator.setSum(accumulator.getSum() + currentVal);


        // 4. 下面统计边界阈值告警规则的计算
        Set<SideEffectMultiMetricUnionRule.MetricRuleItem> metricRules = metricClassRuleMap.get(value.getName());
        if (metricRules != null && !metricRules.isEmpty()) {
            for (SideEffectMultiMetricUnionRule.MetricRuleItem metricRule : metricRules) {
                try {
                    if (metricRule.getCalType().equals(SideEffectMultiMetricUnionRule.CalType.boundCount.name())) {
                        String key = UnionMetricAggResult.genMetricModelKey(metricRule);
                        if (SideEffectMultiMetricUnionRule.MetricRuleItem.boundCount(metricRule.getOp(), metricRule.getValue(), value.getValue())) {
                            long count = accumulator.getMetricModelMap().computeIfAbsent(key, k -> 0L);
                            accumulator.getMetricModelMap().put(key, count + 1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
