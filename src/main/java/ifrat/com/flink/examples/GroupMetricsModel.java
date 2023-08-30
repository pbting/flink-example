package ifrat.com.flink.examples;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@ToString
@Data
public class GroupMetricsModel implements Serializable {

    private String groupKey;
    private long timestamp;
    private Map<String, MetricModel> metrics = new HashMap<>();

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class MetricModel {

        private String name;
        private long value;
    }
}
