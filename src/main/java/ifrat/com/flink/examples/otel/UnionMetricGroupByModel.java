package ifrat.com.flink.examples.otel;

import ifrat.com.flink.examples.SideEffectMultiMetricUnionRule;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Data
public class UnionMetricGroupByModel implements Serializable {

    // 0 表示不是、1 表示 是
    private byte unionGroupKey;
    private String groupKey;
    // 指标名
    private String name;
    private long timestamp;
    //
    private long value;
    private List<SideEffectMultiMetricUnionRule.MetricAlertRule> metricAlertRuleList;
    // 当前这个指标所对应的告警规则的聚合计算集
    private Set<SideEffectMultiMetricUnionRule.MetricRuleItem> metricRuleSet;
}
