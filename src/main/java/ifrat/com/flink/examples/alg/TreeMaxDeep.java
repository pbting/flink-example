package ifrat.com.flink.examples.alg;


import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

// 层序遍历
public class TreeMaxDeep extends BaseTree {

    public static void main(String[] args) {

        TreeMaxDeep tree = new TreeMaxDeep();
        System.out.println("max deep: " + maxDeep(Collections.singletonList(tree.root), 0));
        System.err.println("max deep: " + afterOrder(tree.root));

    }

    // 使用层次遍历，可以解二叉树的最大深度和最小深度
    public static int maxDeep(List<StrTreeNode> levelNodes, int currentDeep) {

        currentDeep++;

        List<StrTreeNode> nextLevelNode = new LinkedList<>();

        for (StrTreeNode node : levelNodes) {

            if (node.leftNode != null) {
                nextLevelNode.add(node.leftNode);
            }

            if (node.rightNode != null) {
                nextLevelNode.add(node.rightNode);
            }
        }

        if (!nextLevelNode.isEmpty()) {
            return maxDeep(nextLevelNode, currentDeep);
        }

        return currentDeep;
    }


    // 使用后序遍历: 左 -> 右 -> 根

    /**
     * 二叉树的高度: 叶子节点到根节点的最长距离 = 二叉树的深度: 根节点到叶子节点的最长距离
     *
     * @param node node
     * @return max deep
     */
    public static int afterOrder(StrTreeNode node) {

        if (node == null) {
            return 0;
        }

        int left = afterOrder(node.leftNode);
        int right = afterOrder(node.rightNode);

        // 到根节点的距离
        return Math.max(left, right) + 1;
    }
}
