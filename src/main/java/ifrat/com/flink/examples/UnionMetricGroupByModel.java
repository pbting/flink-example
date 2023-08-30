package ifrat.com.flink.examples;

import lombok.Data;

import java.io.Serializable;

@Data
public class UnionMetricGroupByModel implements Serializable {

    private String groupKey;
    // 指标名
    private String name;
    private long timestamp;
    //
    private long value;
}
