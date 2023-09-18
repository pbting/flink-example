package ifrat.com.flink.examples;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@ToString
@Data
public class ImitateMetricData implements Serializable {

    private Double value = 0.0D;
    private String lable = "data";
    private String oneLevelGroupKey;
    private String op;
    private String name;
    private long timestamp;
}
