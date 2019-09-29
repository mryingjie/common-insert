package com.github.myyingjie.commoninsert.bean;

import lombok.Data;

import java.util.LinkedHashMap;

/**
 * created by Yingjie Zheng at 2019-09-27 10:35
 */
@Data
public class InsertParam {


    private String host;

    private int port = 3306;

    private String database;

    private String tableName;

    private String userName;

    private String password;

    private int num;

    private String type;

    private String filePath;

    private LinkedHashMap<String,String> increase;

    private LinkedHashMap<String,String> random;

    private LinkedHashMap<String,String> constant;


}
