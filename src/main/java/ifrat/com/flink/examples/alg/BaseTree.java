package ifrat.com.flink.examples.alg;

public class BaseTree {

    StrTreeNode root;
    StrTreeNode otherRoot;


    {
        StrTreeNode root = new StrTreeNode("1");
        StrTreeNode root2 = new StrTreeNode("2");
        StrTreeNode root3 = new StrTreeNode("3");
        StrTreeNode root4 = new StrTreeNode("4");
        StrTreeNode root5 = new StrTreeNode("5");
        StrTreeNode root6 = new StrTreeNode("6");
        StrTreeNode root7 = new StrTreeNode("7");
        StrTreeNode root8 = new StrTreeNode("8");

        root.leftNode = root2;
        root.rightNode = root3;

        root2.leftNode = root4;
        root2.rightNode = root5;

        root5.leftNode = root7;
        root5.rightNode = root8;

        root3.rightNode = root6;

        this.root = root;
    }

    {
        StrTreeNode root = new StrTreeNode("1");
        StrTreeNode root2 = new StrTreeNode("2");
        StrTreeNode root3 = new StrTreeNode("3");
        StrTreeNode root4 = new StrTreeNode("4");
        StrTreeNode root5 = new StrTreeNode("5");
        StrTreeNode root6 = new StrTreeNode("6");
        StrTreeNode root7 = new StrTreeNode("7");
        StrTreeNode root8 = new StrTreeNode("8");

        root.leftNode = root2;
        root.rightNode = root3;

        root2.leftNode = root4;
        root2.rightNode = root5;

        root5.leftNode = root7;
        root5.rightNode = root8;

        root3.rightNode = root6;
        root3.leftNode = new StrTreeNode("9");

        this.otherRoot = root;
    }
}
