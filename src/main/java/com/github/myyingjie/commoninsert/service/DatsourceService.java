package com.github.myyingjie.commoninsert.service;

import com.github.myyingjie.commoninsert.bean.DataSourceProperties;
import com.github.myyingjie.commoninsert.bean.DataSourcePropertiesVo;
import com.github.myyingjie.commoninsert.bean.InsertParam;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * created by Yingjie Zheng at 2019-09-27 11:12
 */
public interface DatsourceService {


    List<DataSourcePropertiesVo> queryDatasource(int status);

    void deleteDatasource(String database) throws IOException;

    void updateDatasource(DataSourceProperties dataSourceProperties) throws Exception;

    void addDatasource(DataSourceProperties dataSourceProperties) throws Exception;

    void persistence(String database) throws IOException;
}
