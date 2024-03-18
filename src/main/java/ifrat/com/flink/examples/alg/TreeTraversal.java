package ifrat.com.flink.examples.alg;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TreeTraversal {

    public static void main(String[] args) {

        TreeNode root = new TreeNode(1);
        TreeNode root2 = new TreeNode(2);
        TreeNode root3 = new TreeNode(3);
        TreeNode root4 = new TreeNode(4);
        TreeNode root5 = new TreeNode(5);
        TreeNode root6 = new TreeNode(6);
        TreeNode root7 = new TreeNode(7);
        TreeNode root8 = new TreeNode(8);

        root.leftNode = root2;
        root.rightNode = root3;

        root2.leftNode = root4;
        root2.rightNode = root5;

        root5.leftNode = root7;
        root5.rightNode = root8;

        root3.rightNode = root6;


        preorderTraversal(root);
        System.err.println();
        middleTraversal(root);
        System.err.println();
        afterTraversal(root);
        System.err.println();
        levelOrderTraversal(Collections.singletonList(root));

    }

    // 前序遍历: 根 -> 左 -> 右
    public static void preorderTraversal(TreeNode node) {

        if (node == null) {
            return;
        }

        // 根
        System.err.print(node.val + " ");
        // 左
        preorderTraversal(node.leftNode);
        // 右
        preorderTraversal(node.rightNode);
    }

    // 中序遍历: 左 -> 根 -> 右
    public static void middleTraversal(TreeNode node) {

        if (node == null) {
            return;
        }

        // 左
        middleTraversal(node.leftNode);
        // 根
        System.err.print(node.val + " ");
        // 右
        middleTraversal(node.rightNode);
    }

    // 后续遍历: 左 -> 右 -> 根
    public static void afterTraversal(TreeNode node) {
        if (node == null) {
            return;
        }

        // 左
        afterTraversal(node.leftNode);
        // 右
        afterTraversal(node.rightNode);
        // 跟
        System.err.print(node.val + " ");
    }

    // 广度优先遍历
    public static void levelOrderTraversal(List<TreeNode> nodes) {

        List<TreeNode> levelNode = new LinkedList<>();
        for (TreeNode node : nodes) {
            // 先输出当前层
            System.err.print(node.val + " ");
            // 收集下一层
            if (node.leftNode != null) {
                levelNode.add(node.leftNode);
            }

            if (node.rightNode != null) {
                levelNode.add(node.rightNode);
            }
        }

        if (!levelNode.isEmpty()) {
            levelOrderTraversal(levelNode);
        }
    }
}

class TreeNode {
    int val;
    TreeNode leftNode;
    TreeNode rightNode;

    TreeNode(int val) {
        this.val = val;
    }
}
