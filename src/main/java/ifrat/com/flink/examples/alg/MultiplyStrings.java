package ifrat.com.flink.examples.alg;

public class MultiplyStrings {
    public static String multiply(String num1, String num2) {
        int m = num1.length();
        int n = num2.length();
        int[] result = new int[m + n];

        for (int i = m - 1; i >= 0; i--) {
            for (int j = n - 1; j >= 0; j--) {
                int product = (num1.charAt(i) - '0') * (num2.charAt(j) - '0');
                int p1 = i + j; // 处理进位
                int p2 = i + j + 1; // 处理当前结果为

                // 最重要的一步: 要加上之前的这一位可能有进位，因此需要加上
                int sum = product + result[p2];

                result[p1] += sum / 10; // 满 10 进位
                result[p2] = sum % 10; // 满 10 求余
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int digit : result) {
            if (!(sb.length() == 0 && digit == 0)) {
                sb.append(digit);
            }
        }

        return sb.length() == 0 ? "0" : sb.toString();
    }

    public static void main(String[] args) {
        String num1 = "123";
        String num2 = "48";
        String product = multiply(num1, num2);
        System.out.println(product);
    }
}
