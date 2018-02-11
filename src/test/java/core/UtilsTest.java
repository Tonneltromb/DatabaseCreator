package core;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class UtilsTest {

    private int[] array ;

    @Before
    public void setUp() {
        array = new int[]{1,2,3,4,5};
    }



    @Test
    public void test_get_random_value_and_array_without_this_value_1() {
        Utils.Pair pair = Utils.getRandomValueAndArrayWithoutThisValue(array, 3);
        assertThat(pair.getArray(), is(new int[]{1, 2, 3, 5}));
        assertThat(pair.getValue(), is(4));
    }

    @Test
    public void test_get_random_value_and_array_without_this_value() {
        Utils.Pair pair = Utils.getRandomValueAndArrayWithoutThisValue(array, 0);
        if (!(pair.getValue() == 1 && Arrays.equals(pair.getArray(), new int[]{2, 3, 4, 5}))) {
            throw new AssertionError();
        }
    }

}