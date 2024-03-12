package ifrat.com.flink.examples.tests;

import java.util.regex.Pattern;

public class PatternMatchTests {

    public static void main(String[] args) {

        // "cos_cls#
        Pattern pattern = Pattern.compile("^cos_cls#.*");

        System.err.println(pattern.matcher("cos_cls#a#b#c").find());

        // tenant + "#" + logSet + "#" + topic.getName();
        // ^[\w\d]+#[\w\d]+#([\w\d]+)?$
        Pattern clsTopicPattern = Pattern.compile("^[\\w\\d]+#[\\w\\d]+#([\\w\\d]+)?$");
//
        System.err.println(pattern.matcher("cos_cls#a#b#c").matches());
        System.err.println(pattern.matcher("cos_cls#a1#b2#c").matches());
        System.err.println(pattern.matcher("cos_cls#1#b#3").matches());
        System.err.println(pattern.matcher("cos_cls#ad#b#c3").matches());
    }

}
