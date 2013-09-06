package com.bangor.exception;

/**
 *
 * @author Joseph W Plant
 */
public class PatternNotDivisibleByGroupSizeException extends Exception{
public PatternNotDivisibleByGroupSizeException(){
        super();
    }
    public PatternNotDivisibleByGroupSizeException(String message){
        super(message);
    }
    public PatternNotDivisibleByGroupSizeException(String message, Throwable cause){
        super(message, cause);
    }
    public PatternNotDivisibleByGroupSizeException(Throwable cause){
        super(cause);
    }
}
