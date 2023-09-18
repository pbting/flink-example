package ifrat.com.flink.examples.otel;

import ifrat.com.flink.examples.SideEffectMultiMetricUnionRule;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

@Data
@ToString
public class UnionMetricAggResult implements Serializable {

    private String groupKey;
    private String name;
    private long timestamp;
    private long count;
    private long minVal;
    private long maxVal;
    private long sum;
    private double avg;
    // 输出的结果里面，可能还含有联合的其他指标
    private Set<List<UnionMetricAggResult>> unionOtherMetric;
    // calType#op#value 三元组组成
    private Map<String, Long> metricModelMap = new HashMap<>();

    // 同时需要处理多个指标联合告警的场景，
    private Map<String, UnionMetricAggResult> unionMetricAggResultMap = new HashMap<>();

    public static String genMetricModelKey(SideEffectMultiMetricUnionRule.MetricRuleItem metricRule) {

        return StringUtils.join(Arrays.asList(metricRule.getCalType(), metricRule.getOp(), metricRule.getValue()), "#");
    }
}
