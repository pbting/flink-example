package ifrat.com.flink.examples;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@Data
@ToString
@NoArgsConstructor
public class RtAvgAccModel implements Serializable {
    private String name;
    private long timestamp;
    private double avg;
    private List<Double> list = new LinkedList<>();


}
