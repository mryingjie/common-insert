package com.github.myyingjie.commoninsert.annotation;
import java.lang.annotation.*;

/**
 * created by Yingjie Zheng at 2019-10-11 10:03
 */
@Target(ElementType.FIELD)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Default {

    String value() default "";

    int intValue() default 0;

    double doubleValue() default 0.0;
}
