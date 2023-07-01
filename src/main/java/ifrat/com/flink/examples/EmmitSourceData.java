package ifrat.com.flink.examples;

import java.io.Serializable;

public class EmmitSourceData implements Serializable {

    private String name;
    private double value;

    public EmmitSourceData(String name,double value){
        this.name = name;
        this.value = value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }
}
