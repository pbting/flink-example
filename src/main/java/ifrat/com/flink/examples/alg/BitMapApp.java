package ifrat.com.flink.examples.alg;

import java.util.Arrays;
import java.util.List;

public class BitMapApp {

    public static void main(String[] args) {

        long bitVal = 0;

        // 3、17、23、43、54
        List<Integer> valList = Arrays.asList(3, 17, 23, 43, 54);

        for (Integer i : valList) {
            bitVal = 1L << i | bitVal;
        }

        System.err.println(Long.toBinaryString(bitVal));
        // 判断 3 存在不存在
        System.err.println("3: " + ((1L << 3 & bitVal) > 0));
        // 判断 4 存在不存在
        System.err.println("4:" + ((1L << 4 & bitVal) > 0));
        // 判断 17 存在不存在
        System.err.println("17: " + ((1L << 17 & bitVal) > 0));
        // 判断 23 存在不存在
        System.err.println("23: " + ((1L << 23 & bitVal) > 0));
        // 判断 43 存在不存在
        System.err.println("43: " + ((1L << 43 & bitVal) > 0));
        // 判断 54 存在不存在
        System.err.println("54: " + ((1L << 54 & bitVal) > 0));
        // 判断 47 存在不存在
        System.err.println("47: " + ((1L << 47 & bitVal) > 0));
        // 判断 55 存在不存在
        System.err.println("55: " + ((1L << 55 & bitVal) > 0));
    }
}
