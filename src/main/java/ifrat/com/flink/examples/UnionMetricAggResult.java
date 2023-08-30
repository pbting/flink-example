package ifrat.com.flink.examples;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@ToString
public class UnionMetricAggResult implements Serializable {

    private long timestamp;
    private String groupKey;

    private Map<String, MultiValueAggMetricModel> metricModelMap = new HashMap<>();

    @Data
    @ToString
    public static class MultiValueAggMetricModel {
        private String name;
        private long count;
        private long minVal;
        private long maxVal;
        private double avg;
        private long sum;
        // and so on
    }
}
