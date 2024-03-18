package ifrat.com.flink.examples.alg;

public class TreeCommonAncestor {

    public static void main(String[] args) {

    }

    /**
     * 遍历寻找公共祖先
     * 1. p、q 在节点的两侧，返回节点
     * 2、 p、q 在节点的左侧，返回节点
     * 3、 p、q 在节点的右侧，返回节点
     */

    public static StrTreeNode lowestCommonAncestor(StrTreeNode node, StrTreeNode p, StrTreeNode q) {

        if (node == null || node.val.equals(p.val) || node.val.equals(q.val)) {
            return node;
        }

        StrTreeNode leftNode = lowestCommonAncestor(node.leftNode, p, q);
        StrTreeNode rightNode = lowestCommonAncestor(node.rightNode, p, q);

        if (leftNode != null && rightNode != null) {
            return node;
        }

        if (leftNode != null) {

            return leftNode;
        }

        return rightNode;
    }
}
