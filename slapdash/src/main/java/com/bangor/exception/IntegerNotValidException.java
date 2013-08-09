package com.bangor.exception;

/**
 *
 * @author joseph
 */
public class IntegerNotValidException extends Exception{
    
    public IntegerNotValidException(){
        super();
    }
    public IntegerNotValidException(String message){
        super(message);
    }
    public IntegerNotValidException(String message, Throwable cause){
        super(message, cause);
    }
    public IntegerNotValidException(Throwable cause){
        super(cause);
    }
    
}
