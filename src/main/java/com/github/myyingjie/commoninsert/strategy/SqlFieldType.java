package com.github.myyingjie.commoninsert.strategy;

import java.util.HashMap;
import java.util.Map;

/**
 * created by Yingjie Zheng at 2019-10-17 17:32
 */
public enum SqlFieldType {

    CHAR("char",FieldType.STRING),

    VARCHAR("varchar",FieldType.STRING),

    TEXT("text",FieldType.STRING),

    INT("int",FieldType.INTEGER),

    INTEGER("integer",FieldType.INTEGER),

    TINYINT("tinyint",FieldType.INTEGER),

    BIGINT("bigint",FieldType.LONG),

    DOUBLE("double",FieldType.DOUBLE),

    FLOAT("float",FieldType.DOUBLE),

    DECIMAL("decimal", FieldType.DECIMAL),

    DATE("date",FieldType.DATE),

    DATETIME("datetime",FieldType.DATETIME),

    TIMESTAMP("timestamp",FieldType.DATETIME);


    private String name;

    private FieldType fieldType;

    private static final Map<String,SqlFieldType> sqlFieldTypeMap;

    static {
        sqlFieldTypeMap = new HashMap<>();
        for (SqlFieldType value : SqlFieldType.values()) {
            sqlFieldTypeMap.put(value.name, value);
        }
    }

    SqlFieldType(String name,FieldType fieldType){
        this.name = name;
        this.fieldType = fieldType;
    }

    public static FieldType transToFieldType(String name){
        return sqlFieldTypeMap.get(name).fieldType;
    }


}
