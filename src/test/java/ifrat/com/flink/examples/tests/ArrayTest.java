package ifrat.com.flink.examples.tests;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class ArrayTest {

    @Test
    public void arrayRandom() {

        final String[] rtArray = new String[]{
                "api_001_rt", "api_002_rt", "api_003_rt", "api_004_rt"
        };

        for (int i = 0; i < 10; i++) {
            System.out.println(rtArray[ThreadLocalRandom.current().nextInt(rtArray.length)]);
        }
    }
}
