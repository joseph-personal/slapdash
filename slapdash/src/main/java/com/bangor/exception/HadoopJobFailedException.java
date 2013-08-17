package com.bangor.exception;

/**
 *
 * @author joseph
 */
public class HadoopJobFailedException extends Exception{
    
    public HadoopJobFailedException(){
        super();
    }
    public HadoopJobFailedException(String message){
        super(message);
    }
    public HadoopJobFailedException(String message, Throwable cause){
        super(message, cause);
    }
    public HadoopJobFailedException(Throwable cause){
        super(cause);
    }
    
}
