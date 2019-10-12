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
public interface InsertService {

    int insert(InsertParam insertParam) throws IOException, SQLException;

    List<DataSourcePropertiesVo> queryDatasource(int status);

    void deleteDatasource(String database);

    void updateDatasource(DataSourceProperties dataSourceProperties);

    void addDatasource(DataSourceProperties dataSourceProperties);

    void persistence(String database) throws IOException;
}
