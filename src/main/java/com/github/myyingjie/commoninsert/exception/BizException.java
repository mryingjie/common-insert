package com.github.myyingjie.commoninsert.exception;

/**
 * created by Yingjie Zheng at 2019-10-18 11:49
 */
public class BizException extends RuntimeException{

    public BizException(String message) {
        super(message);
    }

    public BizException(Throwable cause) {
        super(cause);
    }

    public BizException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
