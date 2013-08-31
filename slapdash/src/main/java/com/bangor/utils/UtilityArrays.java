package com.bangor.utils;

/**
 *
 * @author Joseph W Plant
 */
public class UtilityArrays {
    public static boolean contains(Object[] arr, Object object){
        for (int i = 0; i < arr.length; i++) {
            if(arr[i].equals(object)){
                return true;
            }
        }
        return false;
    }
}
