package ifrat.com.flink.examples;

import java.io.Serializable;

public class DetectResult implements Serializable {

    private String name;
    private double preValue;
    private double value;

    public DetectResult(String name, double preValue, double value) {
        this.name = name;
        this.preValue = preValue;
        this.value = value;
    }

    public double getPreValue() {
        return preValue;
    }

    public void setPreValue(double preValue) {
        this.preValue = preValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DetectResult{" +
                "name='" + name + '\'' +
                ", preValue=" + preValue +
                ", value=" + value +
                '}';
    }
}
