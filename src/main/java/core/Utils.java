package core;

import java.util.*;

public class Utils {

    public static String getRandomString(String[] arr) {
        Random random = new Random();
        return arr[random.nextInt(arr.length)];
    }

    public static int getRandomInt(int[] arr) {
        Random random = new Random();
        return arr[random.nextInt(arr.length)];
    }

    public static int[] getArrayOfRandomValues(int[] arr, int length) {
        int arrLength = arr.length;
        HashMap<Integer, Integer> map = new HashMap<>();
        int[] newArr = new int[length];
        if (length > arr.length) {
            for (int i = 0; i < length; i++) {
                if (i > (length >> 1)) {
                    int sneak = getRandomInt(arr);
                    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                        if (entry.getValue() <= ((length / arrLength)) - (length / (arrLength * arrLength))) {
                            sneak = entry.getKey();
                            break;
                        }
                    }
                    newArr[i] = sneak;
                    map.merge(newArr[i], 1, Integer::sum);
                    continue;
                } else {
                    newArr[i] = getRandomInt(arr);
                    map.merge(newArr[i], 1, Integer::sum);
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                newArr[i] = getRandomInt(arr);
            }
        }
        return newArr;
    }

    public static int getIntegerFromNumbers(int... array) {
        StringBuilder string = new StringBuilder();
        for (int number : array) {
            string.append(Integer.toString(number));
        }
        try {
            return Integer.parseInt(string.toString());
        } catch (NumberFormatException e) {
            e = new NumberFormatException("Is too long value: " + string);
            throw e;
        }
    }

    public static Pair getRandomValueAndArrayWithoutThisValue(int[] array) {
        int index = new Random().nextInt(array.length);
        return getRandomValueAndArrayWithoutThisValue(array, index);
    }

    public static Pair getRandomValueAndArrayWithoutThisValue(int[] array, int index) {
        int value = array[index];
        int[] arrayBefore = Arrays.copyOfRange(array, 0, index);
        int[] arrayAfter = Arrays.copyOfRange(array, index + 1, array.length);
        int[] resultArray = new int[array.length - 1];
        for (int i = 0; i < resultArray.length; i++) {
            if (i > arrayBefore.length - 1) {
                resultArray[i] = arrayAfter[i - index];
            } else {
                resultArray[i] = arrayBefore[i];
            }
        }
        return new Pair(value, resultArray);
    }

    static class Pair {
        private final int value;
        private int[] array;

        Pair(int value, int[] array) {
            this.value = value;
            this.array = array;
        }

        int getValue() {
            return value;
        }

        int[] getArray() {
            return array;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "value=" + value +
                    ", array=" + Arrays.toString(array) +
                    '}';
        }
    }

}