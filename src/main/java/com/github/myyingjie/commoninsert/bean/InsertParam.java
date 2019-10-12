package com.github.myyingjie.commoninsert.bean;

import lombok.Data;

import java.util.LinkedHashMap;

/**
 * created by Yingjie Zheng at 2019-09-27 10:35
 */
@Data
public class InsertParam{

    private String tableName;

    private String database;
    /**
     * 插入的条数
     */
    private int num;


    private LinkedHashMap<String,String> increase;

    private LinkedHashMap<String,String> random;

    private LinkedHashMap<String,String> constant;


}
