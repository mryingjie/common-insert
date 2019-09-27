package com.github.myyingjie.commoninsert.bean;

import com.github.myyingjie.commoninsert.strategy.ConvertStrategy;
import com.heitaox.sql.executor.core.util.DateUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * created by Yingjie Zheng at 2019-09-27 14:28
 */
public enum  FieldType implements ConvertStrategy {


    INTEGER("Integer"){
        @Override
        public Object convert(String value) {
            return Integer.parseInt(value);
        }
    },

    LONG("Long"){
        @Override
        public Object convert(String value) {

            return Long.parseLong(value);
        }
    },

    DATE("Date"){
        @Override
        public Object convert(String value) {
            return LocalDate.parse(value, DateUtils.yyyyMMdd);
        }
    },

    DATETIME("DateTime"){
        @Override
        public Object convert(String value) {
            return LocalDateTime.parse(value, DateUtils.dateTimeFormatter);
        }
    },

    STRING("String"){
        @Override
        public Object convert(String value) {
            return "'"+value+"'";
        }
    },

    DOUBLE("Double"){
        @Override
        public Object convert(String value) {
            return Double.parseDouble(value);
        }
    },

    DECIMAL("Decimal"){
        @Override
        public Object convert(String value) {
            return new BigDecimal(value);
        }
    };


    private String value;

    final static Map<String,FieldType> map;

    FieldType(String value){
        this.value =value;
    }
    static {
        map = new HashMap<>();
        FieldType[] values = FieldType.values();
        for (FieldType fieldType : values) {
            map.put(fieldType.value, fieldType);
        }
    }


    public static FieldType getByValue(String typeValue){
        return map.get(typeValue);
    }

}
