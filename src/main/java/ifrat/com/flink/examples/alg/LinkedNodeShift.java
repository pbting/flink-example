package ifrat.com.flink.examples.alg;

public class LinkedNodeShift {

    public static void main(String[] args) {

        LinkedNode node1 = new LinkedNode(3);
        LinkedNode node2 = new LinkedNode(4);
        LinkedNode node3 = new LinkedNode(5);
        LinkedNode node4 = new LinkedNode(6);
        LinkedNode node5 = new LinkedNode(7);

        node1.nextNode = node2;
        node2.nextNode = node3;
        node3.nextNode = node4;
        node4.nextNode = node5;

        LinkedNode head = rightShift(node1, 5);

        System.err.println(head.val);
    }

    /**
     * 思路:
     * 1. 先遍历一遍，找到尾结点，同时找到截断的节点
     *
     * @param head  head
     * @param count count
     * @return node
     */
    public static LinkedNode rightShift(LinkedNode head, int count) {

        if (head == null) {
            return null;
        }

        if (head.nextNode == null) {
            return head;
        }

        // 第 1 步: 求长度
        int length = 1;
        LinkedNode tailNode = head;
        while (tailNode.nextNode != null) {
            length++;
            tailNode = tailNode.nextNode;
        }

        // 第 2 步: 求步长
        int shift = count % length;

        // 处理移动刚好是长度倍数的情况
        if (shift == 0) {
            return head;
        }
        // 第 3 步: 求新的尾结点
        int shiftStep = length - shift;
        LinkedNode newTailNode = head;
        for (int i = 1; i < shiftStep; i++) {
            newTailNode = newTailNode.nextNode;
        }
        // 第 4 步: 处理链接
        LinkedNode newHeadNode = newTailNode.nextNode;
        tailNode.nextNode = head;

        return newHeadNode;
    }
}
