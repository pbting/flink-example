package ifrat.com.flink.examples.alg;

import java.util.Objects;

public class SameTree extends BaseTree {

    public static void main(String[] args) {

        StrTreeNode node1 = new StrTreeNode("A");
        StrTreeNode node2 = new StrTreeNode("A");

        SameTree same = new SameTree();

        boolean result = isTheSame(same.root, same.otherRoot);

        System.out.println("result: " + result);

    }

    public static boolean isTheSame(StrTreeNode node1, StrTreeNode node2) {

        if (node1 == null && node2 == null) {
            return true;
        }

        if (node1 == null || node2 == null || !Objects.requireNonNull(node1).val.equals(node2.val)) {
            return false;
        }

        // 先遍历左子树
        boolean leftResult = isTheSame(node1.leftNode, node2.leftNode);

        // 只要左子树不相同，则这颗数
        if (!leftResult) {
            return false;
        } else {
            // 后遍历右子树
            return isTheSame(node1.rightNode, node2.rightNode);
        }
    }
}
