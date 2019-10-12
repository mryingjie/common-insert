package com.github.myyingjie.commoninsert.util;

import com.github.myyingjie.commoninsert.annotation.Default;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

/**
 * created by Yingjie Zheng at 2019-10-11 10:08
 */
@Slf4j
public class ReflectUtil {

    public static void setDefaultValue(Object obj,Class aclass){
        Field[] fields = aclass.getDeclaredFields();

        for (Field field : fields) {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            Default annotation = field.getAnnotation(Default.class);
            if (annotation != null) {
                try {
                    Class<?> type = field.getType();
                    Object o = field.get(obj);
                    if(o == null){
                        if(type.equals(String.class)){
                            field.set(obj, annotation.value());
                        }
                        if(type.equals(int.class)){
                            field.set(obj, annotation.intValue());
                        }
                        if(type.equals(double.class)){
                            field.set(obj, annotation.doubleValue());
                        }
                    }


                } catch (IllegalAccessException e) {
                    log.error("反射异常",e);
                }

            }
        }

    }
}
