package com.github.myyingjie.commoninsert.service;

import com.github.myyingjie.commoninsert.bean.InsertParam;
import com.github.myyingjie.commoninsert.bean.InsertParamVo;
import com.github.myyingjie.commoninsert.bean.ParamListVo;

import java.io.IOException;
import java.util.List;

/**
 * created by Yingjie Zheng at 2019-10-12 17:22
 */
public interface InsertService {

    int insert(InsertParam insertParam) throws Exception;

    InsertParamVo detail(String tableName);

    void delete(String tableName) throws IOException;

    List<ParamListVo> paramList();

    InsertParamVo transform(String sql);

    void save(InsertParam insertParam) throws IOException;

    void persistence(String tableName) throws IOException;

}
