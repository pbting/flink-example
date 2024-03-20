package ifrat.com.flink.examples.alg;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 给定 [1,2,3], 输出全排列
 */
public class Permute {

    public static void main(String[] args) {
        int[] source = new int[]{1, 2, 3};
        List<String> result = new LinkedList<>();
        permute(source, new boolean[3], result, new ArrayList<>());
        System.err.println(result);
    }

    /**
     * @param source s
     */
    public static void permute(int[] source, boolean[] used,
                               List<String> result, List<Integer> depth) {

        if (depth.size() == source.length) {
            // 递归退出的条件
            result.add("[ " + StringUtils.join(depth, ",") + " ]");
            return;
        }

        for (int i = 0; i < source.length; i++) {
            if (!used[i]) {
                // 处理当前这一层
                depth.add(source[i]);
                used[i] = true;
                // 递归处理
                permute(source, used, result, depth);
                // 退出当前这一层，回溯，进行下一轮
                used[i] = false;
                depth.remove(depth.size() - 1);
            }
        }
    }
}
