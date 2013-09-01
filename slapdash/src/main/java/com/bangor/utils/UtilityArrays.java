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
    
    public static Object[] swap(Object[] arr, int iIndex1, int iIndex2){
        Object oObjectHolder = arr[iIndex1];
        arr[iIndex1] = arr[iIndex2];
        arr[iIndex2] = oObjectHolder;
        
        return arr;
        
    }
    
    public static void printArray(Object[] arr, String sPrepend){
        for (int i = 0; i < arr.length; i++) {
            System.out.println(sPrepend + "arr["+i+"]: " + arr[i]);
        }
    }
}
