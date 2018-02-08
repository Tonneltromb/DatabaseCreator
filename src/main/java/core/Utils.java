package core;

import java.util.*;

public class Utils {

    public static String getRandonString(String[] arr) {
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
//                        ((length >> 1) - (length >> 2)-(length>>3))
//                        length*(arrLength-1)
                        if (entry.getValue() <=((length/arrLength))-(length/(arrLength*arrLength))) {
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

    public static void main(String[] args) {
        int[] arr1 = {1, 2};
        int[] arr2 = {1, 2, 3};
        int[] arr3 = {1, 2, 3, 4};
        System.out.println(Arrays.toString(Utils.getArrayOfRandomValues(arr1,10)));
        System.out.println(Arrays.toString(Utils.getArrayOfRandomValues(arr2,10)));
        System.out.println(Arrays.toString(Utils.getArrayOfRandomValues(arr3,10)));
//        System.out.println(Utils.getArrayOfRandomValues(arr1, 10));
//        System.out.println(Utils.getArrayOfRandomValues(arr2, 10));
//        System.out.println(Utils.getArrayOfRandomValues(arr3, 10));
    }

}
