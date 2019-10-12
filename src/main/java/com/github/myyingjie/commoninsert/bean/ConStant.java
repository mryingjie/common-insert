package com.github.myyingjie.commoninsert.bean;
import java.util.LinkedHashMap;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

/**
 * created by Yingjie Zheng at 2019-09-27 14:19
 */
public class ConStant {

    public static final String SEPARATOR = "\\|";

    public static final int ANY1 = -1;

    public static final String ANY2 = "-1";


    public static void main(String[] args) {
        List<InsertParam> list = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            InsertParam insertParam = new InsertParam();
            insertParam.setHost("");
            insertParam.setPort(i);
            insertParam.setDatabase("");
            insertParam.setTableName("");
            insertParam.setUserName("");
            insertParam.setPassword("");
            insertParam.setNum(i);
            insertParam.setType("");
            insertParam.setFilePath("");
            list.add(insertParam);
        }

        System.out.println(JSON.toJSONString(list));
    }


}
