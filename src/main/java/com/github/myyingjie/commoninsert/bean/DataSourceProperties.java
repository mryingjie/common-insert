package com.github.myyingjie.commoninsert.bean;

import com.github.myyingjie.commoninsert.annotation.Default;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * created by Yingjie Zheng at 2019-10-09 14:37
 */
@Data
@EqualsAndHashCode
public class DataSourceProperties {

    @Default("--")
    private String database;

    @Default("--")
    private String filePath;

    @Default("--")
    private String host;


    private int port;

    @Default("--")
    private String type;

    @Default("--")
    private String userName;

    @Default("--")
    private String password;

    /**
     * 是否已持久化
     */
    private boolean persistenced = true;

}
