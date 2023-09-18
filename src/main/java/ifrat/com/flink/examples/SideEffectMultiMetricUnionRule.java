package ifrat.com.flink.examples;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.*;

@Data
@ToString
public class SideEffectMultiMetricUnionRule implements Serializable {

    public enum CalType {
        sum,
        min,
        max,
        avg,
        boundCount,
    }

    /**
     * 这里映射的是每一个指标所对应的告警规则的集合
     * <p>
     * 一个告警规则里面可能：
     * - 有一个指标，
     * - 也有可能有多个指标
     */
    private Map<String, MetricAlertRule> alertRuleMetricDemMap = new HashMap<>();

    @Data
    @ToString
    public static class MetricAlertRule {
        // 指标的告警规则，可能含有一个或者多个
        private Set<MetricRuleItem> ruleItemSet = new HashSet<>();
    }

    @Data
    @ToString
    public static class MetricRuleItem {

        // 指标类型: Counter、Guage、Histogram、Summary
        protected String metricType;
        // 哪个指标
        protected String name;
        // 计算类型: sum(求和)、min(最小值)、max(最大值)、avg(求均值)、bound count(边界值统计计数)
        protected String calType = CalType.boundCount.name();
        // 是否需要计算阈值范围，需要的时候，指定 > 或者 < 或者 >=、或者 <=
        protected String op;
        // 指定阈值范围时，具体的阈值是多少,如果没有配置告警，则没有值
        protected int value;

        public static boolean boundCount(String op, int threshold, long value) {

            if (">".equals(op)) {
                return value > threshold;
            }

            if (">=".equals(op)) {
                return value >= threshold;
            }

            if ("<".equals(op)) {
                return value < threshold;
            }

            if ("<=".equals(op)) {
                return value <= threshold;
            }

            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricRuleItem that = (MetricRuleItem) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(calType, that.calType) &&
                    Objects.equals(op, that.op)
                    && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, calType, op, value);
        }
    }

}
