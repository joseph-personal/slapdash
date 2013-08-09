package com.bangor.exception;

/**
 *
 * @author joseph
 */
public class ArrayLengthNotEqualException extends Exception{
    
    public ArrayLengthNotEqualException(){
        super();
    }
    public ArrayLengthNotEqualException(String message){
        super(message);
    }
    public ArrayLengthNotEqualException(String message, Throwable cause){
        super(message, cause);
    }
    public ArrayLengthNotEqualException(Throwable cause){
        super(cause);
    }
    
}
