package ifrat.com.flink.examples;

import java.io.Serializable;

public class EmmitSourceAccData implements Serializable {

    private String name;
    private String op;
    private double val;
    private String windowTimestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public double getVal() {
        return val;
    }

    public void setVal(double val) {
        this.val = val;
    }

    public String getWindowTimestamp() {
        return windowTimestamp;
    }

    public void setWindowTimestamp(String windowTimestamp) {
        this.windowTimestamp = windowTimestamp;
    }

    @Override
    public String toString() {
        return "name:" + this.name + "; val:" + this.val + "; op:" + this.op + "; window timestamp:" + this.windowTimestamp;
    }
}
