package ifrat.com.flink.examples.alg;

import java.util.*;

/**
 * 还原二叉树
 */
public class RestoreTree {

    private static StrTreeNode root;

    public static void main(String[] args) {

        // 前序遍历: A -> B -> D -> F -> G -> H -> I -> E  -> C
        // 中序遍历: F -> D -> H -> G -> I -> B -> E -> A  -> C

        List<String> preOrder = Arrays.asList("A", "B", "D", "F", "G", "H", "I", "E", "C");
        List<String> middleOrder = new ArrayList<>(Arrays.asList("F", "D", "H", "G", "I", "B", "E", "A", "C"));

        Set<String> hasProcess = new HashSet<>();

        loopRestore(hasProcess, 0, preOrder, middleOrder);

        preorderTraversal(root);
    }

    public static void preorderTraversal(StrTreeNode node) {

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


    private static void loopRestore(Set<String> hasProcess, int rootIndex, List<String> preOrder, List<String> middleOrder) {

        if (hasProcess.size() == preOrder.size()) {
            return;
        }

        if (hasProcess.contains(preOrder.get(rootIndex))) {
            loopRestore(hasProcess, rootIndex + 1, preOrder, middleOrder);
        }


        StrTreeNode root = new StrTreeNode(preOrder.get(rootIndex));
        hasProcess.add(root.val);

        if (rootIndex == 0) {
            RestoreTree.root = root;
        }

        int index = middleOrder.indexOf(root.val);
        int leftIndex = index - 1;
        int rightIndex = index + 1;
        if (rightIndex == middleOrder.size() - 1) {
            root.rightNode = new StrTreeNode(middleOrder.get(rightIndex));
            hasProcess.add(middleOrder.get(rightIndex));
        } else {
            root.rightNode = new StrTreeNode(preOrder.get(rootIndex + 1));
            hasProcess.add(preOrder.get(rootIndex + 1));
        }

        if (leftIndex == 0) {
            root.leftNode = new StrTreeNode(middleOrder.get(leftIndex));
            hasProcess.add(middleOrder.get(leftIndex));
        } else {
            root.leftNode = new StrTreeNode(preOrder.get(rootIndex + 1));
            hasProcess.add(preOrder.get(rootIndex + 1));
        }

        hasProcess.forEach(middleOrder::remove);
        loopRestore(hasProcess, rootIndex + 1, preOrder, middleOrder);
    }
}

