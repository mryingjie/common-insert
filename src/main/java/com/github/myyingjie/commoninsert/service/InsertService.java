package com.github.myyingjie.commoninsert.service;

import com.github.myyingjie.commoninsert.bean.InsertParam;

import java.io.IOException;
import java.sql.SQLException;

/**
 * created by Yingjie Zheng at 2019-09-27 11:12
 */
public interface InsertService {

    int insert(InsertParam insertParam) throws IOException, SQLException;
}
