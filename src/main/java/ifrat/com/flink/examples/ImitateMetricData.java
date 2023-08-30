package ifrat.com.flink.examples;

import lombok.Data;

import java.io.Serializable;

@Data
public class ImitateMetricData implements Serializable {

    private String oneLevelGroupKey;
    private String name;
    private Double value = 0.0D;

    private long timestamp;

    @Override
    public String toString() {
        return "Data{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
