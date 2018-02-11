package core;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.runners.Parameterized.*;

@RunWith(Parameterized.class)
public class UtilsTestWithParametrizedRunner {

    @Parameters
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {0, 1, new int[]{2, 3, 4}},
                {1, 2, new int[]{1, 3, 4}},
                {2, 3, new int[]{1, 2, 4}},
                {3, 4, new int[]{1, 2, 3}},

        });
    }

    private int[] inputArray;

    @Parameter(0)
    public int inputIndex;

    @Parameter(1)
    public int expectedValue;

    @Parameter(2)
    public int[] expectedArray;

    @Before
    public void setUp() {
        inputArray = new int[]{1, 2, 3, 4};
    }

    @Test
    public void test_return_value_and_array() {
        Utils.Pair pair = Utils.getRandomValueAndArrayWithoutThisValue(inputArray, inputIndex);
        if (!(pair.getValue() == expectedValue && Arrays.equals(pair.getArray(), expectedArray))) {
            throw new AssertionError();
        }
    }
}
