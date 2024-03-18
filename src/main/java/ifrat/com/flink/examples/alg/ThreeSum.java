package ifrat.com.flink.examples.alg;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ThreeSum {

    public static void main(String[] args) {

        List<Integer> source = Arrays.asList(0, 0, 0);

        List<Integer[]> result = threeSum(source);
        result.forEach(val -> System.out.println(Arrays.asList(val)));
    }

    public static List<Integer[]> threeSum(List<Integer> array) {

        List<Integer[]> result = new LinkedList<>();
        Integer[] val = new Integer[3];
        for (int i = 0; i < array.size() - 2; i++) {
            for (int j = i + 1; j < array.size() - 1; j++) {
                for (int x = j + 1; x < array.size(); x++) {
                    if (array.get(i) + array.get(j) + array.get(x) == 0) {
                        val[0] = array.get(i);
                        val[1] = array.get(j);
                        val[2] = array.get(x);
                        result.add(val);
                        val = new Integer[3];
                    }
                }
            }
        }

        return result;
    }
}
